#include "builtins_internal.h"

/* Destination binding uses the same native comparison and cast as Source.
 * Storage fields belong to the target namespace; row identity remains the source's. */

static ud_status prepare(void *user, const ud_component_context *c, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    *instance = NULL;
    builtin *b = ud_alloc(c->memory, sizeof(*b));
    if (!b)
        return UD_RESOURCE;
    b->kind = (int)(intptr_t)user;
    b->context = *c;
    b->before_column = UINT32_MAX;
    ud_status s = ud_builtin_parse(c, text, &b->config);
    int kind = b->kind;
    uint32_t inputs = c->input_count;
    b->input_count = inputs;
    b->inputs = ud_alloc(c->memory, (inputs + 1) * sizeof(*b->inputs));
    if (!b->inputs)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < inputs; i++)
        s = ud_builtin_clone_schema(c->memory, in[i], b->inputs + i);
    if (!s)
        s = ud_builtin_clone_schema(c->memory, sink_kind(kind) ? in[0] : out[0], &b->schema);
    if (!s && (kind == CAST || kind == XLSX_SOURCE))
        s = ud_builtin_clone_schema(c->memory, out[1], &b->errors);
    if (!s && (kind == SQL_SINK || kind == REST_SINK)) {
        ud_string mode = {0};
        s = ud_json_string(ud_member(b->config, "mode"), &mode);
        b->mode = ud_named(mode, "append")      ? UD_WRITE_APPEND
                  : ud_named(mode, "upsert")    ? UD_WRITE_UPSERT
                  : ud_named(mode, "delete")    ? UD_WRITE_DELETE
                  : ud_named(mode, "changeset") ? UD_WRITE_CHANGESET
                                                : 0;
        if (!s && !b->mode)
            s = UD_SCHEMA;
        ud_string before = {"before", 6};
        if (!s && ud_member(b->config, "beforeField"))
            s = ud_json_string(ud_member(b->config, "beforeField"), &before);
        uint32_t n = in[0]->columns;
        b->write_indices = ud_alloc(c->memory, n * sizeof(*b->write_indices));
        ud_field *fields = ud_alloc(c->memory, n * sizeof(*fields));
        if (!b->write_indices || !fields)
            s = UD_RESOURCE;
        uint32_t count = 0;
        for (uint32_t i = 0; !s && i < n; i++) {
            ud_field f = in[0]->column[i].field;
            if (b->mode == UD_WRITE_CHANGESET && f.name_length == before.length &&
                !memcmp(f.name, before.data, before.length)) {
                if (f.type != UD_STRUCT)
                    s = UD_SCHEMA;
                else
                    b->before_column = i;
            } else {
                b->write_indices[count] = i;
                fields[count++] = f;
            }
        }
        if (!s && b->before_column != UINT32_MAX) {
            const ud_field *f = &in[0]->column[b->before_column].field;
            if (f->child_count != count)
                s = UD_SCHEMA;
            for (uint32_t i = 0; !s && i < count; i++)
                if (!ud_field_matches(f->children + i, fields + i, UD_FIELD_LAYOUT))
                    s = UD_SCHEMA;
        }
        if (!s) {
            ud_batch_destroy(b->schema);
            b->schema = NULL;
            s = ud_batch_create(c->memory, fields, count, 0, &b->schema);
        }
        ud_free(fields);
    }
    if (!s) {
        b->columns = b->schema->columns;
        b->fields = ud_alloc(c->memory, b->columns * sizeof(*b->fields));
        if (!b->fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->columns; i++)
            s = ud_batch_field(b->schema, i, b->fields + i);
    }
    if (!s && (source_kind(kind) || sink_kind(kind))) {
        ud_string connection = {0};
        s = ud_json_string(ud_member(b->config, "connection"), &connection);
        uint64_t caps = source_kind(kind) ? UD_CONNECT_READ : UD_CONNECT_APPEND;
        if (kind == SQL_SINK || kind == REST_SINK)
            caps = b->mode == UD_WRITE_APPEND   ? UD_CONNECT_APPEND
                   : b->mode == UD_WRITE_UPSERT ? UD_CONNECT_UPSERT
                   : b->mode == UD_WRITE_DELETE ? UD_CONNECT_DELETE
                                                : UD_CONNECT_CHANGESET;
        if (kind == SQL_SINK)
            caps |= UD_CONNECT_METADATA | UD_CONNECT_RECEIPTS;
        if (!s)
            s = kind == REST_SOURCE || kind == REST_SINK
                    ? ud_acquire_http_connection(c->activation, connection, &b->lease)
                    : ud_activation_acquire_connection(c->activation, connection, caps, &b->lease);
    }
    uint64_t batch = c->max_batch_rows;
    if (!s && ud_member(b->config, "batchSize"))
        s = ud_json_uint(ud_member(b->config, "batchSize"), &batch);
    if (!s && (!batch || batch > c->max_batch_rows))
        s = UD_SCHEMA;
    if (!s && (file_source(kind) || file_sink(kind))) {
        ud_string path = {0};
        s = ud_connection_lease_path(b->lease, &path);
        if (!s) {
            b->path = ud_strdup_n(c->memory, path.data, path.length);
            if (!b->path)
                s = UD_RESOURCE;
        }
    }
    if (!s && source_kind(kind)) {
        uint64_t count = 1, id = 0, begin = 0, end = UINT64_MAX;
        if (ud_member(b->config, "partitionCount"))
            s = ud_json_uint(ud_member(b->config, "partitionCount"), &count);
        if (!s && ud_member(b->config, "partitionId"))
            s = ud_json_uint(ud_member(b->config, "partitionId"), &id);
        if (!s && ud_member(b->config, "partitionBegin"))
            s = ud_json_uint(ud_member(b->config, "partitionBegin"), &begin);
        if (!s && ud_member(b->config, "partitionEnd"))
            s = ud_json_uint(ud_member(b->config, "partitionEnd"), &end);
        if (!s && (!count || count > 64 || id >= count || begin > end))
            s = UD_SCHEMA;
        b->source_partitions = (uint32_t)count;
        b->source_partition = (uint32_t)id;
        b->source_begin = begin;
        b->source_end = end;
    }
    if (!s && kind == REST_SOURCE)
        s = ud_rest_reader_open(c, b->lease, b->config, b->schema, &b->rest_reader);
    if (!s && kind == REST_SINK)
        s = ud_connection_lease_bind_rest(b->lease, c, b->config);
    if (!s && (file_source(kind)))
        s = ud_checkpoint_source(c->activation, b->path);
    if (!s && (file_source(kind)))
        s = ud_connection_lease_authorize(b->lease, UD_ACCESS_READ);
    if (!s && kind == CSV_SOURCE) {
        ud_string null = {"\\N", 2};
        if (ud_member(b->config, "nullMarker"))
            s = ud_json_string(ud_member(b->config, "nullMarker"), &null);
        ud_csv_options o = {sizeof(o), UD_ABI_VERSION, (uint32_t)batch, null.data, null.length};
        if (!s)
            s = ud_csv_open(c->memory, b->path, b->fields, b->columns, &o, &b->csv);
    }
    if (!s && kind == XLSX_SOURCE) {
        char *configuration = NULL;
        uint32_t length = 0;
        s = ud_json_serialize(b->config, &configuration, &length);
        ud_xlsx_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .batch_size = (uint32_t)batch,
                                   .configuration = {configuration, length},
                                   .buffers = c->execution->buffers,
                                   .check = ud_builtin_operator_check,
                                   .user = c->activation};
        if (!s)
            s = ud_xlsx_reader_open(c->memory, b->path, b->fields, b->columns, &options, &b->xlsx);
        ud_free(configuration);
    }
#ifdef UD_WITH_ARROW_FILES
    if (!s && (kind == PARQUET_SOURCE || kind == IPC_SOURCE || kind == IPC_STREAM_SOURCE)) {
        ud_columnar_file_options options = {.struct_size = sizeof(options),
                                            .abi_version = UD_ABI_VERSION,
                                            .batch_rows = (uint32_t)batch,
                                            .format = kind == PARQUET_SOURCE ? UD_FILE_PARQUET
                                                      : kind == IPC_SOURCE   ? UD_FILE_ARROW_IPC
                                                                             : UD_FILE_ARROW_STREAM,
                                            .buffers = c->execution->buffers,
                                            .check = ud_builtin_operator_check,
                                            .user = c->activation};
        s = ud_columnar_reader_open(c->memory, b->path, b->schema, &options, &b->columnar_reader);
    }
#endif
    if (!s && kind == JSON_SOURCE) {
        ud_json_options o = {sizeof(o), UD_CONFIG_VERSION, (uint32_t)batch, 1048576,
                             UD_ROUND_EXACT};
        s = ud_json_reader_open(c->memory, b->path, b->fields, b->columns, &o, &b->reader);
    }
    if (!s && (file_sink(kind))) {
        size_t len = strlen(b->path);
        char *lock = ud_alloc(c->memory, len + 32);
        if (!lock)
            s = UD_RESOURCE;
        else {
            snprintf(lock, len + 32, "%s.ud-package.lock", b->path);
            s = ud_lock(c->memory, lock, &b->lock);
            ud_free(lock);
        }
        int replaying = 0;
        if (!s) {
            b->temporary = ud_alloc(c->memory, len + 32);
            if (!b->temporary)
                s = UD_RESOURCE;
            else
                snprintf(b->temporary, len + 32, "%s.ud-package.part", b->path);
        }
        if (!s)
            s = ud_checkpoint_stage(c->activation, b->path, b->temporary, &replaying);
        struct stat info;
        if (!s) {
            b->existing = !stat(b->path, &info);
            if (b->existing && !replaying)
                s = UD_BUSY;
            else if (!b->existing && errno != ENOENT)
                s = UD_IO;
        }
        if (!s && replaying)
            s = ud_remove_file(c->memory, b->temporary);
        if (!s) {
            s = ud_reserve_file(c->memory, b->temporary);
            if (!s)
                b->temporary_owned = 1;
        }
        if (!s && (kind == CSV_SINK || kind == JSON_SINK))
            s = kind == CSV_SINK ? ud_csv_writer_open(c->memory, b->temporary, b->fields,
                                                      b->columns, "\\N", &b->csv_writer)
                                 : ud_json_writer_open(c->memory, b->temporary, b->fields,
                                                       b->columns, 1048576, &b->writer);
        if (!s && kind == XLSX_SINK) {
            char *configuration = NULL;
            uint32_t length = 0;
            s = ud_json_serialize(b->config, &configuration, &length);
            ud_xlsx_options options = {.struct_size = sizeof(options),
                                       .abi_version = UD_ABI_VERSION,
                                       .batch_size = c->max_batch_rows,
                                       .configuration = {configuration, length},
                                       .buffers = c->execution->buffers,
                                       .check = ud_builtin_operator_check,
                                       .user = c->activation,
                                       .stage_bytes = &b->stage_bytes};
            if (!s)
                s = ud_xlsx_writer_open(c->memory, b->temporary, b->fields, b->columns, &options,
                                        &b->xlsx_writer);
            ud_free(configuration);
        }
    }
#ifdef UD_WITH_ARROW_FILES
    if (!s && (kind == PARQUET_SINK || kind == IPC_SINK || kind == IPC_STREAM_SINK)) {
        ud_columnar_file_options options = {.struct_size = sizeof(options),
                                            .abi_version = UD_ABI_VERSION,
                                            .batch_rows = c->max_batch_rows,
                                            .format = kind == PARQUET_SINK ? UD_FILE_PARQUET
                                                      : kind == IPC_SINK   ? UD_FILE_ARROW_IPC
                                                                           : UD_FILE_ARROW_STREAM,
                                            .buffers = c->execution->buffers,
                                            .check = ud_builtin_operator_check,
                                            .user = c->activation,
                                            .stage_bytes = &b->stage_bytes};
        s = ud_columnar_writer_open(c->memory, b->temporary, b->schema, &options,
                                    &b->columnar_writer);
    }
#endif
    if (!s && kind == SQL_SINK && ud_member(b->config, "provisionReceipts")) {
        const char *flag = NULL;
        uint32_t n = 0;
        s = ud_json_text(ud_member(b->config, "provisionReceipts"), &flag, &n);
        if (!s && ud_json_kind(ud_member(b->config, "provisionReceipts")) != UD_JSON_BOOL)
            s = UD_SCHEMA;
        if (!s && n == 4)
            s = ud_connection_lease_provision(b->lease);
    }
    if (!s && (kind == SQL_SINK || kind == REST_SINK)) {
        const ud_json *keys = ud_member(b->config, "keys");
        if (keys && ud_json_kind(keys) != UD_JSON_ARRAY)
            s = UD_SCHEMA;
        b->key_count = ud_json_size(keys);
        b->keys = ud_alloc(c->memory, (b->key_count + 1) * sizeof(*b->keys));
        if (!b->keys)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->key_count; i++) {
            uint64_t key;
            s = ud_json_uint(ud_json_at(keys, i), &key);
            if (!s && key >= b->columns)
                s = UD_SCHEMA;
            if (!s)
                b->keys[i] = (uint32_t)key;
        }
        if (!s && kind == SQL_SINK)
            s = ud_builtin_sink_schema(b);
        else if (!s && kind == REST_SINK) {
            s = ud_builtin_clone_schema(c->memory, b->schema, &b->actual_schema);
            if (!s)
                s = ud_builtin_clone_schema(c->memory, b->schema, &b->write_schema);
            b->schema_policy =
                (ud_string){ud_strdup_n(c->memory, "{\"policy\":\"strict\"}", 19), 19};
            if (!b->schema_policy.data)
                s = UD_RESOURCE;
        }
        ud_activation *e = c->activation;
        if (!s && !e->execution->options.execution_id.length)
            s = UD_INVALID;
        size_t len = e->execution->options.execution_id.length + c->task_id.length +
                     c->component_id.length + 64;
        if (!s) {
            b->job = ud_alloc(c->memory, len);
            if (!b->job)
                s = UD_RESOURCE;
            else
                snprintf(b->job, len, "%.*s/%.*s/%.*s",
                         (int)e->execution->options.execution_id.length,
                         e->execution->options.execution_id.data, (int)c->task_id.length,
                         c->task_id.data, (int)c->component_id.length, c->component_id.data);
        }
    }
    if (!s && (kind == DERIVE || kind == SPLIT)) {
        const ud_json *columns = ud_member(b->config, "columns");
        const ud_json *branches = ud_member(b->config, "branches");
        b->expression_count =
            kind == SPLIT ? (branches ? ud_json_size(branches) : 1) : ud_json_size(columns);
        b->expressions = ud_alloc(c->memory, b->expression_count * sizeof(*b->expressions));
        if (!b->expressions)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->expression_count; i++)
            s = ud_builtin_bind_expr(
                c,
                kind == SPLIT ? (branches ? ud_member(ud_json_at(branches, i), "condition")
                                          : ud_member(b->config, "condition"))
                              : ud_member(ud_json_at(columns, i), "expression"),
                in[0], b->expressions + i);
    }
    if (!s && (kind == SORT || kind == JOIN || kind == WINDOW || kind == AGGREGATE))
        s = ud_builtin_create_operator(c, kind, b->config, in, &b->operator);
    if (!s && kind == SQL_SOURCE)
        s = ud_builtin_parameters(c, b->config, &b->parameters);
    if (!s && source_kind(kind))
        s = ud_builtin_source_prepare(b, (uint32_t)batch);
    if (s) {
        ud_builtin_dispose(b, s);
        return s;
    }
    *instance = b;
    return UD_OK;
}

static ud_status step(void *ptr, const ud_component_event *event) {
    builtin *b = ptr;
    int kind = b->kind;
    UD_TRY(ud_activation_check(b->context.activation));
    if (source_kind(kind)) {
        UD_TRY(ud_connection_lease_authorize(b->lease, UD_ACCESS_READ));
        ud_batch *batch = NULL;
        ud_status s = ud_builtin_source_next(b, &batch);
        if (!s) {
            if (!s && kind == XLSX_SOURCE && b->xlsx_rejected) {
                ud_batch *errors = NULL;
                s = ud_batch_create(b->context.memory, b->errors->schema->fields,
                                    b->errors->columns, batch->rows, &errors);
                for (uint32_t row = 0; !s && row < batch->rows; row++)
                    s = ud_error_append(&b->context.errors, errors, batch, row,
                                        (ud_string){"xlsx.cell", 9}, 1, (ud_id){0}, UD_PARSE,
                                        UD_ERROR_REDIRECT);
                if (!s)
                    s = ud_batch_seal(errors);
                if (!s)
                    s = b->context.emit(b->context.emit_user, 1, errors);
                ud_batch_destroy(errors);
            } else if (!s && batch->rows)
                s = b->context.emit(b->context.emit_user, 0, batch);
            else if (!s)
                s = UD_MORE;
        }
        ud_batch_destroy(batch);
        return s;
    }
    if (kind == JOIN) {
        if (event->kind == UD_COMPONENT_DATA)
            UD_TRY(ud_operator_push(b->operator, event->input, event->batch));
        else if (event->kind == UD_COMPONENT_INPUT_END)
            UD_TRY(ud_operator_finish_input(b->operator, event->input));
        return ud_builtin_blocking(b);
    }
    if (event->kind == UD_COMPONENT_INPUT_END)
        return b->operator? ud_operator_finish_input(b->operator, event->input) : UD_OK;
    if (event->kind == UD_COMPONENT_POLL)
        return kind == SORT || kind == JOIN || kind == WINDOW || kind == AGGREGATE
                   ? ud_builtin_blocking(b)
                   : UD_END;
    if (file_sink(kind)) {
        UD_TRY(ud_connection_lease_authorize(b->lease, UD_ACCESS_WRITE));
        ud_status status;
#ifdef UD_WITH_ARROW_FILES
        if (b->columnar_writer)
            status = ud_columnar_writer_append(b->columnar_writer, event->batch);
        else
#endif
            status = kind == CSV_SINK    ? ud_csv_writer_append(b->csv_writer, event->batch)
                     : kind == XLSX_SINK ? ud_xlsx_writer_append(b->xlsx_writer, event->batch)
                                         : ud_json_writer_append(b->writer, event->batch);
        if (!status)
            status = ud_rule_stage(b->context.activation, event->batch, &b->delivery);
        if (!status) {
            b->written_rows += event->batch->rows;
            b->written_errors += ud_batch_error_rows(event->batch);
        }
        return status;
    }
    if (kind == SQL_SINK || kind == REST_SINK) {
        ud_batch *mapped = NULL, *before = NULL;
        ud_status status = ud_builtin_sink_batch(b, event->batch, &mapped, &before);
        if (!status)
            status = ud_builtin_sink_write(b, mapped, before);
        ud_batch_destroy(mapped);
        ud_batch_destroy(before);
        return status;
    }
    if (kind == SORT || kind == JOIN || kind == WINDOW || kind == AGGREGATE)
        return ud_operator_push(b->operator, event->input, event->batch);
    return ud_builtin_transformed(b, event->batch);
}

static ud_status finish(void *ptr) {
    builtin *b = ptr;
    return file_source(b->kind) ? ud_builtin_source_finish(b)
           : file_sink(b->kind) ? ud_builtin_sink_finish(b)
                                : UD_OK;
}

static const char *configuration_schema(int kind) {
    switch (kind) {
    case REST_SOURCE:
        return "{ "
               "\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"table\":{\"type\":\"string\"},\"batchSize\":{"
               "\"type\":\"integer\",\"minimum\":1,\"maximum\":65536},\"caConnection\":{\"type\":"
               "\"string\"},\"url\":{\"type\":\"string\"},\"rowsField\":{\"type\":\"string\","
               "\"default\":\"rows\"},\"cursorField\":{\"type\":\"string\",\"default\":"
               "\"nextCursor\"},\"cursorParameter\":{\"type\":\"string\",\"default\":\"cursor\"},"
               "\"maxPages\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":1000000,\"default\":"
               "10000},\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000},"
               "\"maxResponseBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},"
               "\"maxAttempts\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":8},"
               "\"retryDelayMs\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":60000},"
               "\"incremental\":{\"type\":\"object\"},\"ordering\":{\"type\":\"object\"}},"
               "\"required\":[\"connection\",\"schema\"],\"additionalProperties\":false}";
    case REST_SINK:
        return "{ "
               "\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},\"table\":"
               "{\"type\":\"string\",\"default\":\"rest\"},\"mode\":{\"type\":\"string\",\"enum\":["
               "\"append\",\"upsert\",\"delete\",\"changeset\"],\"default\":\"append\"},\"keys\":{"
               "\"type\":\"array\",\"items\":{\"type\":\"integer\",\"minimum\":0},\"default\":[]},"
               "\"beforeField\":{\"type\":\"string\"},\"receiptUrl\":{\"type\":\"string\","
               "\"minLength\":1},\"caConnection\":{\"type\":\"string\"},\"url\":{\"type\":"
               "\"string\"},\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000}"
               ",\"maxResponseBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},"
               "\"maxAttempts\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":8},"
               "\"retryDelayMs\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":60000}},"
               "\"required\":[\"connection\",\"receiptUrl\"],\"additionalProperties\":false}";
    case PARQUET_SOURCE:
    case IPC_SOURCE:
    case IPC_STREAM_SOURCE:
    case CSV_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"batchSize\":{\"type\":\"integer\"},"
               "\"partitionCount\":{\"type\":\"integer\"},\"partitionId\":{\"type\":\"integer\"},"
               "\"partitionBegin\":{\"type\":\"integer\"},\"partitionEnd\":{\"type\":\"integer\"},"
               "\"incremental\":{\"type\":\"object\"},\"schemaPolicy\":{\"type\":\"object\"},"
               "\"table\":{\"type\":\"string\"},\"nullMarker\":{\"type\":\"string\"}},\"required\":"
               "[\"connection\",\"schema\"],\"additionalProperties\":false}";
    case JSON_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"batchSize\":{\"type\":\"integer\"},"
               "\"partitionCount\":{\"type\":\"integer\"},\"partitionId\":{\"type\":\"integer\"},"
               "\"partitionBegin\":{\"type\":\"integer\"},\"partitionEnd\":{\"type\":\"integer\"},"
               "\"incremental\":{\"type\":\"object\"},\"schemaPolicy\":{\"type\":\"object\"},"
               "\"table\":{\"type\":\"string\"}},\"required\":[\"connection\",\"schema\"],"
               "\"additionalProperties\":false}";
    case SQL_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\","
               "\"minLength\":1,\"x-role\":\"connection-resource\"},\"schema\":{\"type\":"
               "\"object\"},\"batchSize\":{\"type\":\"integer\",\"minimum\":1},\"partitionCount\":{"
               "\"type\":\"integer\",\"minimum\":1,\"maximum\":64},\"partitionId\":{\"type\":"
               "\"integer\",\"minimum\":0},\"partitionBegin\":{\"type\":\"integer\",\"minimum\":0},"
               "\"partitionEnd\":{\"type\":\"integer\",\"minimum\":0},\"incremental\":{\"type\":"
               "\"object\"},\"schemaPolicy\":{\"type\":\"object\"},\"table\":{\"type\":\"string\","
               "\"minLength\":1},\"sql\":{\"type\":\"string\",\"minLength\":1},\"parameters\":{"
               "\"type\":\"object\"},\"ordering\":{\"type\":\"object\"},\"where\":{}},\"required\":"
               "[\"connection\",\"schema\"],\"anyOf\":[{\"required\":[\"sql\"]},{\"required\":["
               "\"table\"]}],\"additionalProperties\":false}";
    case PARQUET_SINK:
    case IPC_SINK:
    case IPC_STREAM_SINK:
    case CSV_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"}},"
               "\"required\":[\"connection\"],\"additionalProperties\":false}";
    case JSON_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"}},"
               "\"required\":[\"connection\"],\"additionalProperties\":false}";
    case SQL_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"table\":{\"type\":\"string\"},\"mode\":{\"type\":\"string\",\"enum\":[\"append\","
               "\"upsert\",\"delete\",\"changeset\"]},\"beforeField\":{\"type\":\"string\"},"
               "\"keys\":{\"type\":"
               "\"array\"},\"schemaPolicy\":{\"type\":\"object\"},\"provisionReceipts\":{\"type\":"
               "\"boolean\"}},\"required\":[\"connection\","
               "\"table\",\"mode\"],\"additionalProperties\":false}";
    case CAST:
        return "{\"type\":\"object\",\"properties\":{\"schema\":{\"type\":\"object\"},\"rounding\":"
               "{\"type\":\"integer\"},\"errorPolicy\":{\"type\":\"string\"},\"includeInput\":{"
               "\"type\":\"boolean\"}},\"required\":[\"schema\"],\"additionalProperties\":false}";
    case DERIVE:
        return "{\"type\":\"object\",\"properties\":{\"columns\":{\"type\":\"array\"}},"
               "\"required\":[\"columns\"],\"additionalProperties\":false}";
    case SPLIT:
        return "{\"type\":\"object\",\"properties\":{\"condition\":{\"type\":\"object\"},"
               "\"branches\":{\"type\":\"array\"},\"default\":{\"type\":\"string\"}},"
               "\"additionalProperties\":false}";
    case SORT:
    case JOIN:
    case AGGREGATE:
    case WINDOW: {
        ud_string schema = {0};
        int k = kind == SORT        ? UD_OPERATOR_SORT
                : kind == JOIN      ? UD_OPERATOR_JOIN
                : kind == AGGREGATE ? UD_OPERATOR_AGGREGATE
                                    : UD_OPERATOR_WINDOW;
        return ud_operator_configuration_schema(k, &schema) ? NULL : schema.data;
    }
    case MULTICAST:
        return "{\"type\":\"object\",\"properties\":{\"outputs\":{\"type\":\"array\"}},"
               "\"additionalProperties\":false}";
    case UNION_ALL:
        return "{\"type\":\"object\",\"properties\":{\"inputs\":{\"type\":\"array\"}},"
               "\"additionalProperties\":false}";
    case XLSX_SOURCE:
        return "{\"type\":\"object\",\"required\":[\"connection\",\"schema\"],\"properties\":{"
               "\"connection\":{\"type\":\"string\"},\"schema\":{\"type\":\"object\"},\"sheet\":{"
               "\"type\":\"string\"},\"range\":{\"type\":\"string\"},\"header\":{\"type\":"
               "\"boolean\"},\"datePolicy\":{\"type\":\"string\"},\"formulaPolicy\":{\"type\":"
               "\"string\"},\"errorPolicy\":{\"type\":\"string\"},\"includeInput\":{\"type\":"
               "\"boolean\"},\"batchSize\":{\"type\":\"integer\"},\"maxCompressedBytes\":{\"type\":"
               "\"integer\"},\"maxDecompressedBytes\":{\"type\":\"integer\"},\"maxEntries\":{"
               "\"type\":\"integer\"},\"maxCells\":{\"type\":\"integer\"},\"maxStringBytes\":{"
               "\"type\":\"integer\"},\"ordering\":{\"type\":\"object\"}},\"additionalProperties\":"
               "false}";
    case XLSX_SINK:
        return "{\"type\":\"object\",\"required\":[\"connection\"],\"properties\":{\"connection\":{"
               "\"type\":\"string\"},\"sheet\":{\"type\":\"string\"},\"header\":{\"type\":"
               "\"boolean\"},\"datePolicy\":{\"type\":\"string\"},\"maxCompressedBytes\":{\"type\":"
               "\"integer\"}},\"additionalProperties\":false}";
    default:
        return "{\"type\":\"object\",\"additionalProperties\":false}";
    }
}

ud_status ud_builtin_register(ud_registry *registry) {
    UD_TRY(ud_relational_register(registry));
    UD_TRY(ud_cdc_register(registry));
    UD_TRY(ud_advanced_register(registry));
    static const struct {
        const char *id;
        int kind;
        uint32_t inputs, outputs;
        int sync, error;
    } entries[] = {{"file.csv.source", CSV_SOURCE, 0, 1, -1, -1},
                   {"file.ndjson.source", JSON_SOURCE, 0, 1, -1, -1},
                   {"sql.source", SQL_SOURCE, 0, 1, -1, -1},
                   {"file.csv.destination", CSV_SINK, 1, 0, -1, -1},
                   {"file.ndjson.destination", JSON_SINK, 1, 0, -1, -1},
                   {"sql.destination", SQL_SINK, 1, 0, -1, -1},
                   {"pass", PASS, 1, 1, 0, -1},
                   {"cast", CAST, 1, 2, -1, 1},
                   {"derive", DERIVE, 1, 1, -1, -1},
                   {"split", SPLIT, 1, 2, -1, -1},
                   {"multicast", MULTICAST, 1, 2, 0, -1},
                   {"unionAll", UNION_ALL, 2, 1, -1, -1},
                   {"sort", SORT, 1, 1, -1, -1},
                   {"join", JOIN, 2, 1, -1, -1},
                   {"aggregate", AGGREGATE, 1, 1, -1, -1},
                   {"window", WINDOW, 1, 1, -1, -1},
                   {"file.xlsx.source", XLSX_SOURCE, 0, 2, -1, 1},
                   {"file.xlsx.destination", XLSX_SINK, 1, 0, -1, -1}
#ifdef UD_WITH_HTTP
                   ,
                   {"rest.source", REST_SOURCE, 0, 1, -1, -1},
                   {"rest.destination", REST_SINK, 1, 0, -1, -1}
#endif
#ifdef UD_WITH_ARROW_FILES
                   ,
                   {"file.parquet.source", PARQUET_SOURCE, 0, 1, -1, -1},
                   {"file.parquet.destination", PARQUET_SINK, 1, 0, -1, -1},
                   {"file.arrowIpc.source", IPC_SOURCE, 0, 1, -1, -1},
                   {"file.arrowIpc.destination", IPC_SINK, 1, 0, -1, -1},
                   {"file.arrowStream.source", IPC_STREAM_SOURCE, 0, 1, -1, -1},
                   {"file.arrowStream.destination", IPC_STREAM_SINK, 1, 0, -1, -1}
#endif
    };
    for (uint32_t i = 0; i < sizeof(entries) / sizeof(*entries); i++) {
        int kind = entries[i].kind;
        int category = source_kind(kind) ? UD_COMPONENT_SOURCE
                       : sink_kind(kind) ? UD_COMPONENT_DESTINATION
                                         : UD_COMPONENT_TRANSFORM;
        ud_component_descriptor d = {
            .struct_size = sizeof(d),
            .abi_version = UD_ABI_VERSION,
            .id = {entries[i].id, (uint32_t)strlen(entries[i].id)},
            .name = {entries[i].id, (uint32_t)strlen(entries[i].id)},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {configuration_schema(kind),
                                     (uint32_t)strlen(configuration_schema(kind))},
            .category = category,
            .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE,
            .inputs = entries[i].inputs,
            .outputs = entries[i].outputs,
            .synchronous_input = entries[i].sync,
            .error_output = entries[i].error,
            .optional_outputs = kind == CAST || kind == XLSX_SOURCE ? UINT64_C(2) : 0,
            .user = (void *)(intptr_t)kind,
            .configure = ud_builtin_configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .confirm = source_kind(kind) ? ud_builtin_confirm : NULL,
            .diagnostics = source_kind(kind) || kind == SQL_SINK ? ud_builtin_diagnostics : NULL,
            .dispose = ud_builtin_dispose};
        if (kind == JOIN)
            d.input_demand = ud_builtin_builtin_demand;
        if (kind == SPLIT || kind == MULTICAST || kind == UNION_ALL)
            d.bind_ports = ud_builtin_builtin_ports;
        if (source_kind(kind))
            d.describe_plan = ud_builtin_describe_source;
        if (kind == SQL_SOURCE)
            d.capabilities |= UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING;
        if (kind == SORT || kind == JOIN || kind == AGGREGATE || kind == WINDOW)
            d.capabilities |= UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING |
                              UD_COMPONENT_CLONEABLE | UD_COMPONENT_MERGEABLE;
        if (kind == SORT || kind == JOIN || kind == AGGREGATE || kind == WINDOW)
            d.describe_plan = ud_builtin_describe_operator;
        if (kind == SQL_SINK || kind == REST_SINK) {
            d.effects = UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_TRANSACTIONAL | UD_EFFECT_RECEIPTED |
                        UD_EFFECT_COMMIT_UNKNOWN;
            d.state_version = 1;
            d.checkpoint = ud_builtin_checkpoint_state;
            d.restore = ud_builtin_restore_state;
            d.capabilities |= UD_COMPONENT_STATEFUL;
        } else if (source_kind(kind))
            d.effects = UD_EFFECT_EXTERNAL_READ;
        else if (file_sink(kind))
            d.effects = UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_COMMIT_UNKNOWN;
        if (kind == SORT || kind == JOIN || kind == AGGREGATE || kind == WINDOW) {
            d.state_version = 1;
            d.checkpoint = ud_builtin_checkpoint_state;
            d.restore = ud_builtin_restore_state;
        }
        UD_TRY(ud_registry_register(registry, &d));
    }
    static const char sql_task_schema[] =
        "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\",\"minLength\":1,"
        "\"x-role\":\"resource-reference\"},\"statement\":{\"type\":\"string\"},\"parameters\":{"
        "\"type\":\"object\",\"properties\":{\"schema\":{\"type\":\"object\"},\"rows\":{\"type\":"
        "\"array\",\"minItems\":1,\"maxItems\":65536}},\"required\":[\"schema\",\"rows\"],"
        "\"additionalProperties\":false}},\"required\":[\"connection\",\"statement\"],"
        "\"additionalProperties\":false}";
    ud_component_descriptor task = {
        .struct_size = sizeof(task),
        .abi_version = UD_ABI_VERSION,
        .id = {"sql.execute", 11},
        .name = {"Execute SQL", 11},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .configuration_schema = {sql_task_schema, sizeof(sql_task_schema) - 1},
        .category = UD_COMPONENT_TASK,
        .synchronous_input = -1,
        .error_output = -1,
        .configure = ud_builtin_sql_task_configure,
        .capabilities = UD_COMPONENT_THREAD_SAFE,
        .invoke = ud_builtin_sql_task};
    UD_TRY(ud_registry_register(registry, &task));
    UD_TRY(ud_transforms_register(registry));
    UD_TRY(ud_quality_component_register(registry));
    UD_TRY(ud_cache_components_register(registry));
    UD_TRY(ud_enumerators_register(registry));
    UD_TRY(ud_filesystem_register(registry));
    UD_TRY(ud_process_register(registry));
    UD_TRY(ud_http_register(registry));
    return ud_profile_register(registry);
}
