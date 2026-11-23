#include "ud/columnar_file.h"
#include "parquet_scan.h"
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include <atomic>
#include <cstring>
#include <new>
#include <vector>
/* Arrow and Parquet are format codecs. The Native context accounts for their
 * bulk memory; Columnar is the only type/NULL/Decimal conversion owner. */
class NativePool final:public arrow::MemoryPool {
    ud_context*ctx_;std::atomic<int64_t> live_{0},peak_{0},total_{0},count_{0};
public:
    explicit NativePool(ud_context*ctx):ctx_(ctx){}
    arrow::Status Allocate(int64_t size,int64_t alignment,uint8_t**out) override {
        if(size<0||alignment<8||alignment>65536||(alignment&(alignment-1)))return arrow::Status::Invalid("allocation bound");
        if(ud_is_cancelled(ctx_))return arrow::Status::Cancelled("cancelled");
        if(static_cast<uint64_t>(size)>SIZE_MAX-static_cast<uint64_t>(alignment)-sizeof(void*))return arrow::Status::OutOfMemory("quota");
        void*base=ud_alloc(ctx_,static_cast<size_t>(size+alignment+sizeof(void*)));if(!base)return arrow::Status::OutOfMemory("quota");
        uintptr_t address=(reinterpret_cast<uintptr_t>(base)+sizeof(void*)+alignment-1)&~static_cast<uintptr_t>(alignment-1);*out=reinterpret_cast<uint8_t*>(address);reinterpret_cast<void**>(*out)[-1]=base;
        auto live=live_.fetch_add(size)+size;auto peak=peak_.load();while(live>peak&&!peak_.compare_exchange_weak(peak,live)){}total_.fetch_add(size);count_.fetch_add(1);return arrow::Status::OK();
    }
    arrow::Status Reallocate(int64_t old_size,int64_t new_size,int64_t alignment,uint8_t**ptr) override {uint8_t*next=nullptr;auto status=Allocate(new_size,alignment,&next);if(!status.ok())return status;std::memcpy(next,*ptr,static_cast<size_t>(old_size<new_size?old_size:new_size));Free(*ptr,old_size,alignment);*ptr=next;return arrow::Status::OK();}
    void Free(uint8_t*ptr,int64_t size,int64_t) override {if(ptr){ud_free(reinterpret_cast<void**>(ptr)[-1]);live_.fetch_sub(size);}}
    int64_t bytes_allocated()const override{return live_.load();}int64_t max_memory()const override{return peak_.load();}int64_t total_bytes_allocated()const override{return total_.load();}int64_t num_allocations()const override{return count_.load();}std::string backend_name()const override{return "UniversalData context quota";}
};
static ud_status native_status(const arrow::Status&s){return s.ok()?UD_OK:s.IsOutOfMemory()?UD_RESOURCE:s.IsCancelled()?UD_CANCELLED:s.IsNotImplemented()?UD_UNSUPPORTED:s.IsIOError()?UD_IO:UD_CORRUPT;}
static ud_status check(ud_context*c,const ud_columnar_file_options&o){if(ud_is_cancelled(c))return UD_CANCELLED;return o.check?o.check(o.user):UD_OK;}
static ud_status import_native(ud_context*c,const std::shared_ptr<arrow::RecordBatch>&batch,ud_batch**out){ArrowArray array{};ArrowSchema schema{};auto status=arrow::ExportRecordBatch(*batch,&array,&schema);ud_status result=native_status(status);if(!result)result=ud_batch_import_arrow(c,&array,&schema,out);if(array.release)array.release(&array);if(schema.release)schema.release(&schema);return result;}
static arrow::Result<std::shared_ptr<arrow::RecordBatch>> export_native(const ud_batch*b){ArrowArray array{};ArrowSchema schema{};ud_status result=ud_batch_export_arrow(b,&array,&schema);if(result)return arrow::Status::Invalid("typed Batch export failed");auto batch=arrow::ImportRecordBatch(&array,&schema);if(array.release)array.release(&array);if(schema.release)schema.release(&schema);return batch;}
struct ud_columnar_reader {
    ud_context*ctx;ud_columnar_file_options options;NativePool pool;
    std::shared_ptr<arrow::io::ReadableFile> input;
    std::unique_ptr<parquet::arrow::FileReader> parquet;
    std::shared_ptr<arrow::RecordBatchReader> stream;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file;
    std::shared_ptr<arrow::RecordBatch> pending;
    ud_batch*expected=nullptr;int64_t offset=0;int index=0;
    bool empty_selection = false;
    ud_columnar_reader(ud_context*c,const ud_columnar_file_options&o):ctx(c),options(o),pool(c){}
    ~ud_columnar_reader(){ud_batch_destroy(expected);}
};
static ud_status select_parquet_columns(ud_columnar_reader *reader, const ud_batch *requested,
                                        int32_t month_from, int32_t month_before) {
    const auto metadata = reader->parquet->parquet_reader()->metadata();
    const auto *schema = metadata->schema();
    std::vector<int> columns;
    for (uint32_t i = 0; i < ud_batch_columns(requested); ++i) {
        ud_field field{};
        ud_status status = ud_batch_field(requested, i, &field);
        if (status) return status;
        int column = schema->ColumnIndex(std::string(field.name, field.name_length));
        if (column < 0 || schema->Column(column)->max_repetition_level() != 0)
            return UD_INCOMPATIBLE;
        columns.push_back(column);
    }
    const int month = schema->ColumnIndex("calendarMonth");
    const bool check_dates = (month_from != INT32_MIN || month_before != INT32_MAX) &&
        month >= 0 && schema->Column(month)->logical_type()->is_date();
    std::vector<int> groups;
    for (int g = 0; g < metadata->num_row_groups(); ++g) {
        if (check_dates) {
            const auto column = metadata->RowGroup(g)->ColumnChunk(month);
            const auto stats = std::dynamic_pointer_cast<parquet::Int32Statistics>(column->statistics());
            if (stats && stats->HasMinMax() &&
                (stats->max() < month_from || stats->min() >= month_before)) continue;
        }
        groups.push_back(g);
    }
    reader->empty_selection = groups.empty();
    if (reader->empty_selection) return UD_OK;
    auto selected = reader->parquet->GetRecordBatchReader(groups, columns);
    if (!selected.ok()) return native_status(selected.status());
    reader->stream = std::move(*selected);
    return UD_OK;
}

static ud_status open_reader(ud_context*c,const char*path,const ud_batch*expected,
                             const ud_columnar_file_options*o, bool project,
                             int32_t month_from, int32_t month_before, ud_columnar_reader**out){
    if(!c||!path||!expected||!o||!out||o->struct_size!=sizeof(*o)||o->abi_version!=UD_ABI_VERSION||!o->batch_rows||o->batch_rows>65536||o->format<1||o->format>3)return UD_INVALID;*out=nullptr;ud_status result=check(c,*o);if(result)return result;
    void*storage=ud_alloc(c,sizeof(ud_columnar_reader));if(!storage)return UD_RESOURCE;auto*r=new(storage)ud_columnar_reader(c,*o);
    try{auto input=arrow::io::ReadableFile::Open(path,&r->pool);if(!input.ok())result=native_status(input.status());else r->input=*input;
        if (!result && o->format == UD_FILE_PARQUET) {
            parquet::ReaderProperties props(&r->pool);
            parquet::ArrowReaderProperties typed;
            typed.set_use_threads(false);
            typed.set_batch_size(o->batch_rows);
            parquet::arrow::FileReaderBuilder builder;
            auto status = builder.Open(r->input, props);
            if (status.ok()) {
                builder.memory_pool(&r->pool);
                builder.properties(typed);
                status = builder.Build(&r->parquet);
            }
            result = native_status(status);
            if (!result && project)
                result = select_parquet_columns(r, expected, month_from, month_before);
            else if (!result) {
                auto stream = r->parquet->GetRecordBatchReader();
                result = native_status(stream.status());
                if (!result) r->stream = std::move(*stream);
            }
        }
        else if(!result){auto options=arrow::ipc::IpcReadOptions::Defaults();options.memory_pool=&r->pool;options.use_threads=false;options.max_recursion_depth=32;
            if(o->format==UD_FILE_ARROW_IPC){auto file=arrow::ipc::RecordBatchFileReader::Open(r->input,options);if(file.ok())r->file=*file;else result=native_status(file.status());}
            else{auto stream=arrow::ipc::RecordBatchStreamReader::Open(r->input,options);if(stream.ok())r->stream=*stream;else result=native_status(stream.status());}}
        std::vector<ud_field> fields(ud_batch_columns(expected));for(uint32_t i=0;!result&&i<fields.size();i++)result=ud_batch_field(expected,i,&fields[i]);if(!result)result=ud_batch_create(c,fields.data(),static_cast<uint32_t>(fields.size()),0,&r->expected);
    }catch(const std::bad_alloc&){result=UD_RESOURCE;}catch(...){result=UD_CORRUPT;}
    if(result){r->~ud_columnar_reader();ud_free(r);return result;}*out=r;return UD_OK;
}
extern "C" ud_status ud_columnar_reader_open(ud_context*c,const char*path,const ud_batch*expected,const ud_columnar_file_options*o,ud_columnar_reader**out){
    return open_reader(c, path, expected, o, false, INT32_MIN, INT32_MAX, out);
}
extern "C" ud_status ud_parquet_scan_open(ud_context*c,const char*path,const ud_batch*expected,
        const ud_columnar_file_options*o,int32_t month_from,int32_t month_before,ud_columnar_reader**out){
    if (!o || o->format != UD_FILE_PARQUET || month_from >= month_before) return UD_INVALID;
    return open_reader(c, path, expected, o, true, month_from, month_before, out);
}
extern "C" ud_status ud_columnar_reader_next(ud_columnar_reader*r,ud_batch**out){if(!r||!out)return UD_INVALID;*out=nullptr;ud_status result=check(r->ctx,r->options);if(result)return result;
    if (r->empty_selection) return UD_END;
    try{while(!r->pending||r->offset==r->pending->num_rows()){r->pending.reset();r->offset=0;
            if(r->file){if(r->index==r->file->num_record_batches())return UD_END;auto batch=r->file->ReadRecordBatch(r->index++);if(!batch.ok())return native_status(batch.status());r->pending=*batch;}
            else{auto status=r->stream->ReadNext(&r->pending);if(!status.ok())return native_status(status);if(!r->pending)return UD_END;}if(!r->pending->num_rows())continue;}
        auto count=std::min<int64_t>(r->options.batch_rows,r->pending->num_rows()-r->offset);auto part=r->pending->Slice(r->offset,count);ud_batch*decoded=nullptr;result=import_native(r->ctx,part,&decoded);
        uint32_t columns=ud_batch_columns(r->expected);std::vector<uint32_t> mapping(columns);std::vector<ud_field> fields(columns);
        if(!result&&ud_batch_columns(decoded)!=columns)result=UD_INCOMPATIBLE;
        for(uint32_t i=0;!result&&i<columns;i++){mapping[i]=i;ud_field actual{};result=ud_batch_field(r->expected,i,&fields[i]);if(!result)result=ud_batch_field(decoded,i,&actual);if(!result&&(actual.name_length!=fields[i].name_length||std::memcmp(actual.name,fields[i].name,actual.name_length)))result=UD_INCOMPATIBLE;}
        if(!result)result=ud_batch_seal(decoded);if(!result)result=ud_batch_project(r->ctx,decoded,mapping.data(),fields.data(),columns,out);ud_batch_destroy(decoded);if(!result)r->offset+=count;
    }catch(const std::bad_alloc&){result=UD_RESOURCE;}catch(...){result=UD_CORRUPT;}return result;
}
extern "C" void ud_columnar_reader_close(ud_columnar_reader*r){if(r){r->~ud_columnar_reader();ud_free(r);}}
struct ud_columnar_writer {
    ud_context*ctx;ud_columnar_file_options options;NativePool pool;
    std::shared_ptr<arrow::io::FileOutputStream> output;std::shared_ptr<arrow::Schema> schema;
    std::unique_ptr<parquet::arrow::FileWriter> parquet;std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc;
    uint64_t reserved=0;bool failed=false;
    ud_columnar_writer(ud_context*c,const ud_columnar_file_options&o):ctx(c),options(o),pool(c){}
};
static ud_status reserve(ud_columnar_writer*w,uint64_t bytes){if(!w->options.buffers||!w->options.stage_bytes)return UD_INVALID;if(UINT64_MAX-w->reserved<bytes)return UD_OVERFLOW;ud_status result=ud_buffer_reserve_external(w->options.buffers,bytes);if(!result){w->reserved+=bytes;*w->options.stage_bytes+=bytes;}return result;}
extern "C" ud_status ud_columnar_writer_open(ud_context*c,const char*path,const ud_batch*schema,const ud_columnar_file_options*o,ud_columnar_writer**out){if(!c||!path||!schema||!o||!out||o->struct_size!=sizeof(*o)||o->abi_version!=UD_ABI_VERSION||o->format<1||o->format>3)return UD_INVALID;*out=nullptr;ud_status result=check(c,*o);if(result)return result;void*storage=ud_alloc(c,sizeof(ud_columnar_writer));if(!storage)return UD_RESOURCE;auto*w=new(storage)ud_columnar_writer(c,*o);
    try{auto batch=export_native(schema);if(!batch.ok())result=native_status(batch.status());else w->schema=(*batch)->schema();size_t wire=0;if(!result){uint8_t*bytes=nullptr;uint64_t n=0;result=ud_batch_serialize(schema,&bytes,&n);wire=static_cast<size_t>(n);ud_free(bytes);}if(!result)result=reserve(w,wire*4+65536);
        if(!result){auto output=arrow::io::FileOutputStream::Open(path);if(output.ok())w->output=*output;else result=native_status(output.status());}
        if (!result && o->format == UD_FILE_PARQUET) {
            // Bound buffered rows per sink when one publication writes multiple shards.
            auto props = parquet::WriterProperties::Builder()
                .memory_pool(&w->pool)->max_row_group_length(32768)
                ->compression(parquet::Compression::UNCOMPRESSED)->build();
            auto arrow_props = parquet::ArrowWriterProperties::Builder()
                .store_schema()->disable_compliant_nested_types()->set_use_threads(false)->build();
            auto writer = parquet::arrow::FileWriter::Open(*w->schema, &w->pool, w->output, props, arrow_props);
            if (writer.ok()) w->parquet = std::move(*writer);
            else result = native_status(writer.status());
        }
        else if(!result){auto options=arrow::ipc::IpcWriteOptions::Defaults();options.memory_pool=&w->pool;options.use_threads=false;auto writer=o->format==UD_FILE_ARROW_IPC?arrow::ipc::MakeFileWriter(w->output,w->schema,options):arrow::ipc::MakeStreamWriter(w->output,w->schema,options);if(writer.ok())w->ipc=*writer;else result=native_status(writer.status());}
    }catch(const std::bad_alloc&){result=UD_RESOURCE;}catch(...){result=UD_CORRUPT;}if(result){w->~ud_columnar_writer();ud_free(w);return result;}*out=w;return UD_OK;
}
extern "C" ud_status ud_columnar_writer_append(ud_columnar_writer*w,const ud_batch*b){if(!w||!b||w->failed)return UD_INVALID;ud_status result=check(w->ctx,w->options);if(result)return result;
    try{uint8_t*bytes=nullptr;uint64_t size=0;result=ud_batch_serialize(b,&bytes,&size);ud_free(bytes);if(!result)result=reserve(w,size*2+65536);if(result){w->failed=true;return result;}auto batch=export_native(b);if(!batch.ok())result=native_status(batch.status());else if(!(*batch)->schema()->Equals(*w->schema,true))result=UD_SCHEMA;else result=native_status(w->parquet?w->parquet->WriteRecordBatch(**batch):w->ipc->WriteRecordBatch(**batch));if(!result)result=check(w->ctx,w->options);if(result)w->failed=true;
    }catch(const std::bad_alloc&){result=UD_RESOURCE;}catch(...){result=UD_CORRUPT;}return result;
}
extern "C" ud_status ud_columnar_writer_close(ud_columnar_writer*w){if(!w)return UD_INVALID;ud_status result=w->failed?UD_IO:check(w->ctx,w->options);try{if(w->parquet){auto status=w->parquet->Close();if(!result)result=native_status(status);}if(w->ipc){auto status=w->ipc->Close();if(!result)result=native_status(status);}if(w->output){auto status=w->output->Close();if(!result)result=native_status(status);}}catch(...){result=UD_IO;}w->~ud_columnar_writer();ud_free(w);return result;}
