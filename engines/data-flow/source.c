#include "builtins_internal.h"
#include "ud/sql.h"
static ud_status open_file_reader(builtin *b, uint32_t batch) {
    const ud_component_context *c = &b->context;
    int kind = b->kind;
    ud_status s = UD_OK;
    if (!s && kind == CSV_SOURCE) {
        ud_string null = {"\\N", 2};
        if (ud_member(b->config, "nullMarker"))
            s = ud_json_string(ud_member(b->config, "nullMarker"), &null);
        ud_csv_options o = {.struct_size = sizeof(o),
                            .abi_version = UD_ABI_VERSION,
                            .batch_size = batch,
                            .null_marker = null.data,
                            .null_marker_length = null.length,
                            .max_record_bytes = 16777216,
                            .records = &b->records};
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
        ud_json_options o = {sizeof(o), UD_CONFIG_VERSION, (uint32_t)batch,
                             1048576,   UD_ROUND_EXACT,    &b->records};
        ud_string framing = {"lines", 5};
        if (ud_member(b->config, "framing"))
            s = ud_json_string(ud_member(b->config, "framing"), &framing);
        o.framing = ud_named(framing, "array")   ? UD_JSON_FRAMING_ARRAY
                    : ud_named(framing, "lines") ? UD_JSON_FRAMING_LINES
                                                 : -1;
        if (!s && o.framing < 0)
            s = UD_SCHEMA;
        if (!s)
            s = ud_json_reader_open(c->memory, b->path, b->fields, b->columns, &o, &b->reader);
    }
    return s;
}
static ud_status filter_rows(builtin *b, ud_batch **input) {
    if (!b->predicate || b->predicate_pushed)
        return UD_OK;
    ud_batch *condition = NULL, *filtered = NULL;
    ud_status status = ud_batch_seal(*input);
    if (!status)
        status = ud_expression_evaluate_batch(b->predicate, *input,
                                              (ud_string){"sourcePredicate", 15}, &condition);
    if (!status)
        status = ud_batch_create(b->context.memory, (*input)->schema->fields, (*input)->columns,
                                 (*input)->rows, &filtered);
    for (uint32_t row = 0; !status && row < (*input)->rows; row++) {
        ud_value value;
        status = ud_batch_get(condition, row, 0, &value);
        if (!status && !value.is_null && value.integer)
            status = ud_batch_append_projected(filtered, *input, row, NULL);
    }
    if (!status)
        status = ud_batch_seal(filtered);
    ud_batch_destroy(condition);
    if (!status) {
        ud_batch_destroy(*input);
        *input = filtered;
        filtered = NULL;
    }
    ud_batch_destroy(filtered);
    return status;
}
static ud_status bind_predicate(builtin *b) {
    const ud_component_context *c = &b->context;
    const ud_json *where = ud_member(b->config, "where");
    if (where) {
        UD_TRY(ud_builtin_bind_expr(c, where, b->schema, &b->predicate));
        if (ud_expression_type(b->predicate) != UD_BOOL)
            return UD_SCHEMA;
    }
    const ud_json *raw = ud_member(b->config, "sql");
    if (raw) {
        ud_string sql;
        UD_TRY(ud_json_string(raw, &sql));
        b->read_sql = (ud_string){ud_strdup_n(c->memory, sql.data, sql.length), sql.length};
        if (!b->read_sql.data)
            return UD_RESOURCE;
        return UD_OK;
    }
    if (b->parameters)
        return UD_SCHEMA;
    ud_string table;
    UD_TRY(ud_json_string(ud_member(b->config, "table"), &table));
    const ud_batch *actual = b->actual_schema ? b->actual_schema : b->schema;
    int32_t dialect = ud_connection_lease_dialect(b->lease);
    UD_TRY(ud_sql_compile_scan(c->memory, dialect, table, actual, &b->read_sql));
    int proof = b->predicate && dialect == UD_SQL_POSTGRESQL &&
                ud_connection_lease_parameter_style(b->lease) == UD_SQL_PARAMETER_NUMBERED &&
                actual->columns == b->schema->columns;
    /* Strong PostgreSQL primitive columns have no provider range/encoding or
     * timestamp conversions that filtering could conceal. Other shapes retain
     * the same Native vector predicate after full value decoding. */
    for (uint32_t i = 0; proof && i < actual->columns; i++)
        proof =
            (actual->column[i].field.type == UD_BOOL || actual->column[i].field.type == UD_INT64) &&
            ud_field_matches(&actual->column[i].field, &b->schema->column[i].field,
                             UD_FIELD_VALUE_TYPE);
    if (proof) {
        ud_string predicate = {0};
        ud_batch *parameters = NULL;
        ud_status status = ud_expression_sql(b->predicate, dialect, &predicate, &parameters);
        if (status == UD_UNSUPPORTED)
            return UD_OK;
        if (status)
            return status;
        ud_bytes sql = {.ctx = c->memory};
        status = ud_bytes_put(&sql, b->read_sql.data, b->read_sql.length);
        if (!status)
            status = ud_bytes_put(&sql, " WHERE ", 7);
        if (!status)
            status = ud_bytes_put(&sql, predicate.data, predicate.length);
        if (!status)
            status = ud_bytes_put(&sql, "", 1);
        ud_free((void *)predicate.data);
        if (!status) {
            ud_free((void *)b->read_sql.data);
            b->read_sql = (ud_string){(char *)sql.data, (uint32_t)sql.size - 1};
            sql.data = NULL;
            b->parameters = parameters;
            parameters = NULL;
            b->predicate_pushed = 1;
        }
        ud_batch_destroy(parameters);
        ud_bytes_clear(&sql);
        return status;
    }
    return UD_OK;
}
ud_status ud_builtin_source_raw(builtin *b, ud_batch **out) {
    ud_batch *raw = NULL;
    ud_status status;
    if (b->rest_reader)
        status = ud_rest_reader_next(b->rest_reader, &raw);
    else
#ifdef UD_WITH_ARROW_FILES
        if (b->columnar_reader)
        status = ud_columnar_reader_next(b->columnar_reader, &raw);
    else
#endif
        status = b->kind == CSV_SOURCE    ? ud_csv_next(b->csv, &raw, &b->source_errors)
                 : b->kind == JSON_SOURCE ? ud_json_reader_next(b->reader, &raw, &b->source_errors)
                 : b->kind == XLSX_SOURCE ? ud_xlsx_reader_next(b->xlsx, &raw, &b->xlsx_rejected)
                                          : ud_connection_lease_read_next(b->lease, &raw);
    if (!status && b->actual_schema) {
        status = ud_schema_map(b->context.memory, raw, b->schema, b->schema_policy, out);
        ud_batch_destroy(raw);
    } else
        *out = raw;
    if (!status && b->kind == SQL_SOURCE)
        status = filter_rows(b, out);
    if (!status && b->source_errors) {
        ud_batch *partitioned = NULL;
        status = ud_builtin_source_partition(b, b->source_errors, &partitioned);
        b->source_errors = partitioned;
        if (!status && !b->source_errors->rows) {
            ud_batch_destroy(b->source_errors);
            b->source_errors = NULL;
        }
    }
    if (!status && b->kind != SQL_SOURCE && b->kind != REST_SOURCE && b->kind != XLSX_SOURCE &&
        b->kind != CSV_SOURCE && b->kind != JSON_SOURCE) {
        ud_batch *identified = NULL;
        status = ud_batch_seal(*out);
        if (!status)
            status = ud_batch_identify(b->context.memory, *out, b->source_scope,
                                       b->source_identified, &identified);
        if (!status)
            b->source_identified += (*out)->rows;
        ud_batch_destroy(*out);
        *out = identified;
    }
    return status;
}

ud_status ud_builtin_source_identity(builtin *b, ud_batch *input, ud_batch **out) {
    ud_status status = ud_batch_seal(input);
    if (!status)
        status = b->cursor && b->cursor->count
                     ? ud_batch_identify_keys(b->context.memory, input, b->source_scope,
                                              b->cursor->keys, b->cursor->count, out)
                     : ud_batch_identify(b->context.memory, input, b->source_scope,
                                         b->source_identified, out);
    if (!status)
        b->source_identified += input->rows;
    ud_batch_destroy(input);
    return status;
}

ud_status ud_builtin_snapshot_store(builtin *b, ud_batch *batch, uint32_t port) {
    const ud_component_context *context = &b->context;
    ud_status status = port >= context->output_count ? UD_SCHEMA
                       : !port && b->cursor          ? ud_cursor_validate(b->cursor, batch)
                                                     : UD_OK;
    if (!status)
        status = ud_batch_seal(batch);
    if (!status && b->snapshot_count == b->snapshot_capacity) {
        uint32_t capacity = b->snapshot_capacity ? b->snapshot_capacity * 2 : 32;
        if (capacity < b->snapshot_capacity || (uint64_t)capacity * sizeof(uint32_t) > SIZE_MAX)
            status = UD_RESOURCE;
        uint32_t *ports =
            !status ? ud_alloc(context->memory, (size_t)capacity * sizeof(*ports)) : NULL;
        ud_buffer_block **blocks =
            !status && !context->execution->journal
                ? ud_alloc(context->memory, (size_t)capacity * sizeof(*blocks))
                : NULL;
        if (!status && (!ports || (!context->execution->journal && !blocks)))
            status = UD_RESOURCE;
        if (!status) {
            if (b->snapshot_count) {
                memcpy(ports, b->snapshot_ports, (size_t)b->snapshot_count * sizeof(*ports));
                if (blocks)
                    memcpy(blocks, b->snapshot, (size_t)b->snapshot_count * sizeof(*blocks));
            }
            ud_free(b->snapshot_ports);
            ud_free(b->snapshot);
            b->snapshot_ports = ports;
            b->snapshot = blocks;
            b->snapshot_capacity = capacity;
        } else {
            ud_free(ports);
            ud_free(blocks);
        }
    }
    if (!status) {
        uint8_t tag[8] = {0};
        tag[0] = (uint8_t)port;
        ud_sha256_update(&b->snapshot_hash, tag, sizeof(tag));
        status = ud_batch_hash_rows(batch, &b->snapshot_hash);
    }
    if (!status && context->execution->journal)
        status = ud_checkpoint_snapshot_write(context->activation, context->component_id,
                                              b->snapshot_count, batch);
    else if (!status)
        status =
            ud_buffer_store(context->execution->buffers, batch, b->snapshot + b->snapshot_count);
    if (!status) {
        b->snapshot_ports[b->snapshot_count++] = port;
        if (!port)
            b->snapshot_rows += batch->rows;
        else
            b->snapshot_error_rows += batch->rows;
    }
    ud_batch_destroy(batch);
    return status;
}

ud_status ud_builtin_source_sealed(builtin *b, const uint8_t digest[32]) {
    memcpy(b->snapshot_fingerprint, digest, 32);
    if (b->source_partitions > 1)
        UD_TRY(ud_source_version(b->context.activation, b->source_scope, digest));
    if (b->kind == SQL_SOURCE && (!b->cursor || !b->cursor->count)) {
        ud_bytes bytes = {.ctx = b->context.memory};
        ud_status status = ud_put_u64(&bytes, b->source_scope.high);
        if (!status)
            status = ud_put_u64(&bytes, b->source_scope.low);
        if (!status)
            status = ud_bytes_put(&bytes, digest, 32);
        if (!status) {
            uint8_t hash[32];
            ud_hash(bytes.data, bytes.size, hash);
            b->source_scope = (ud_id){0};
            for (uint32_t i = 0; i < 8; i++) {
                b->source_scope.high = (b->source_scope.high << 8) | hash[i];
                b->source_scope.low = (b->source_scope.low << 8) | hash[i + 8];
            }
        }
        ud_bytes_clear(&bytes);
        UD_TRY(status);
    }
    if (b->kind == SQL_SOURCE) {
        ud_connection_lease *grant = NULL;
        UD_TRY(ud_connection_lease_snapshot_grant(b->lease, b->context.activation, &grant));
        ud_status status = ud_connection_lease_release(b->lease);
        if (status) {
            ud_connection_lease_release(grant);
            return status;
        }
        b->lease = grant;
    }
    return UD_OK;
}

ud_status ud_builtin_source_prepare(builtin *b, uint32_t batch_rows) {
    const ud_component_context *c = &b->context;
    ud_status status = UD_OK;
    char namespace_id[33];
    ud_string logical_connection = {0}, logical_table = {0};
    status = ud_json_string(ud_member(b->config, "connection"), &logical_connection);
    if (!status)
        status = ud_assets_bind_source(c->activation, logical_connection, b->lease);
    if (!status && ud_member(b->config, "table"))
        status = ud_json_string(ud_member(b->config, "table"), &logical_table);
    if (!status)
        status = ud_connection_field_namespace(c->activation, logical_connection, logical_table,
                                               namespace_id);
    if (!status && b->kind == SQL_SOURCE && ud_member(b->config, "table")) {
        ud_string table;
        status = ud_json_string(ud_member(b->config, "table"), &table);
        if (!status)
            status = ud_connection_lease_schema(b->lease, table, (ud_string){namespace_id, 32},
                                                &b->actual_schema);
        char *policy = NULL;
        uint32_t length = 0;
        if (!status && ud_member(b->config, "schemaPolicy"))
            status = ud_json_serialize(ud_member(b->config, "schemaPolicy"), &policy, &length);
        else if (!status) {
            policy = ud_strdup_n(c->memory, "{\"policy\":\"strict\"}", 19);
            length = 19;
            if (!policy)
                status = UD_RESOURCE;
        }
        b->schema_policy = (ud_string){policy, length};
        uint32_t diff_length = 0;
        if (!status)
            status = ud_schema_compare(c->memory, b->schema, b->actual_schema, b->schema_policy,
                                       &b->schema_diff, &diff_length);
        ud_json *diff = NULL;
        if (!status)
            status = ud_json_parse(c->memory, b->schema_diff, diff_length, &diff, NULL);
        if (!status) {
            const char *p = NULL;
            uint32_t n = 0;
            status = ud_json_text(ud_member(diff, "compatible"), &p, &n);
            if (!status && (n != 4 || memcmp(p, "true", 4)))
                status = UD_INCOMPATIBLE;
        }
        ud_json_destroy(diff);
        if (b->schema_diff) {
            ud_status recorded =
                ud_schema_report(c->activation, (ud_string){b->schema_diff, diff_length}, status);
            if (!status)
                status = recorded;
        }
    } else if (!status && ud_member(b->config, "schemaPolicy"))
        status = UD_SCHEMA;
    if (!status && b->kind == SQL_SOURCE)
        status = bind_predicate(b);
    if (!status)
        status = ud_checkpoint_schema(c->activation, c->component_id,
                                      b->actual_schema ? b->actual_schema : b->schema);
    const ud_json *incremental = ud_member(b->config, "incremental");
    if (!status && incremental) {
        ud_string mode;
        status = ud_json_string(ud_member(incremental, "mode"), &mode);
        if (!status) {
            char *json = NULL;
            uint32_t length = 0;
            status = ud_json_serialize(incremental, &json, &length);
            if (!status)
                status =
                    ud_cursor_create(c->memory, b->schema, (ud_string){json, length}, &b->cursor);
            ud_free(json);
        }
        if (!status && b->cursor && (ud_transaction_active(c->activation)))
            status = UD_UNSUPPORTED;
    }
    /* The immutable snapshot and physical batching are part of the replay policy. */
    char *configuration = NULL;
    uint32_t configuration_length = 0;
    if (!status)
        status = ud_json_serialize(b->config, &configuration, &configuration_length);
    if (!status) {
        uint8_t hash[32];
        ud_hash(configuration, configuration_length, hash);
        static const char hex[] = "0123456789abcdef";
        for (uint32_t i = 0; i < 32; i++) {
            b->source_policy[2 * i] = hex[hash[i] >> 4];
            b->source_policy[2 * i + 1] = hex[hash[i] & 15];
        }
        b->source_policy[64] = 0;
    }
    ud_free(configuration);
    ud_bytes identity = {.ctx = c->memory};
    ud_string connection = {0};
    if (!status)
        status = ud_json_string(ud_member(b->config, "connection"), &connection);
    if (!status)
        status = ud_put_string(&identity, c->execution->options.tenant.data,
                               c->execution->options.tenant.length);
    ud_string resource = {0};
    if (!status)
        status = ud_connection_lease_resource(b->lease, &resource);
    if (!status)
        status = ud_put_string(&identity, resource.data, resource.length);
    if (!status && b->kind == SQL_SOURCE) {
        ud_string sql = b->read_sql;
        if (!status)
            status = ud_put_string(&identity, logical_table.length ? logical_table.data : sql.data,
                                   logical_table.length ? logical_table.length : sql.length);
    }
    if (!status && b->path)
        status = ud_connection_lease_authorize(b->lease, UD_ACCESS_READ);
    if (!status && b->path) {
        uint64_t started = ud_now_ms();
        status = ud_file_hash(c->memory, b->path, b->file_fingerprint);
        b->file_hash_ms += ud_now_ms() - started;
        if (!status)
            status = ud_bytes_put(&identity, b->file_fingerprint, 32);
    }
    if (!status && b->cursor && b->cursor->count)
        status = ud_bytes_put(&identity, b->cursor->fingerprint, 32);
    if (!status) {
        uint8_t hash[32];
        ud_hash(identity.data, identity.size, hash);
        for (uint32_t i = 0; i < 8; i++) {
            b->source_scope.high = (b->source_scope.high << 8) | hash[i];
            b->source_scope.low = (b->source_scope.low << 8) | hash[i + 8];
        }
    }
    ud_bytes_clear(&identity);
    if (!status && (b->kind == CSV_SOURCE || b->kind == JSON_SOURCE)) {
        b->records = (ud_record_options){.struct_size = sizeof(ud_record_options),
                                         .abi_version = UD_ABI_VERSION,
                                         .errors = c->errors,
                                         .source_scope = b->source_scope};
        status = ud_error_policy(b->config, "errorPolicy", UD_ERROR_FAIL, &b->records.policy);
        const ud_json *flag = ud_member(b->config, "includeInput");
        if (!status && flag) {
            const char *value;
            uint32_t length;
            if (ud_json_kind(flag) != UD_JSON_BOOL)
                status = UD_SCHEMA;
            else {
                status = ud_json_text(flag, &value, &length);
                b->records.include_input = length == 4;
            }
        }
    }
    if (!status && file_source(b->kind))
        status = open_file_reader(b, batch_rows);
    b->materialized = b->cursor != NULL || b->kind == SQL_SOURCE || b->kind == REST_SOURCE;
    ud_sha256_init(&b->snapshot_hash);
    if (!status) {
        uint8_t schema[32];
        status = ud_schema_fingerprint(b->schema, schema);
        if (!status)
            ud_sha256_update(&b->snapshot_hash, schema, 32);
    }
    if (!status && b->materialized) {
        char *info = NULL;
        uint32_t length = 0;
        ud_status saved = ud_checkpoint_snapshot_info(
            c->activation, c->component_id, (ud_string){b->source_policy, 64}, &info, &length);
        if (saved == UD_OK) {
            ud_json *header = NULL;
            status = ud_json_parse(c->memory, info, length, &header, NULL);
            if (!status)
                status = ud_json_uint(ud_member(header, "batches"), &b->snapshot_count);
            if (!status)
                status = ud_json_uint(ud_member(header, "rows"), &b->snapshot_rows);
            if (!status)
                status = ud_json_uint(ud_member(header, "errorRows"), &b->snapshot_error_rows);
            const ud_json *ports = ud_member(header, "ports");
            if (!status &&
                (b->snapshot_count > UINT32_MAX || ud_json_kind(ports) != UD_JSON_ARRAY ||
                 ud_json_size(ports) != b->snapshot_count))
                status = UD_CORRUPT;
            if (!status && b->snapshot_count) {
                b->snapshot_ports =
                    ud_alloc(c->memory, (size_t)b->snapshot_count * sizeof(*b->snapshot_ports));
                if (!b->snapshot_ports)
                    status = UD_RESOURCE;
                b->snapshot_capacity = (uint32_t)b->snapshot_count;
            }
            for (uint32_t index = 0; !status && index < b->snapshot_count; index++) {
                uint64_t port;
                status = ud_json_uint(ud_json_at(ports, index), &port);
                if (!status && (port > 1 || port >= c->output_count || (port && !b->errors)))
                    status = UD_CORRUPT;
                if (!status)
                    b->snapshot_ports[index] = (uint32_t)port;
            }
            if (!status) {
                ud_string hash;
                status = ud_json_string(ud_member(header, "fingerprint"), &hash);
                uint8_t fingerprint[32];
                if (!status && hash.length != 64)
                    status = UD_CORRUPT;
                for (uint32_t j = 0; !status && j < 32; j++) {
                    unsigned value = 0;
                    for (uint32_t k = 0; k < 2; k++) {
                        char ch = hash.data[j * 2 + k];
                        unsigned digit = ch >= '0' && ch <= '9'   ? ch - '0'
                                         : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                                                                  : 16;
                        if (digit > 15)
                            status = UD_CORRUPT;
                        value = value * 16 + digit;
                    }
                    fingerprint[j] = (uint8_t)value;
                }
                if (!status)
                    status = ud_builtin_source_sealed(b, fingerprint);
            }
            ud_json_destroy(header);
            ud_free(info);
            return status;
        }
        if (saved != UD_END)
            return saved;
    }
    if (!status && b->kind == SQL_SOURCE) {
        ud_string sql = b->read_sql;
        ud_read_options options = {
            sizeof(options), UD_ABI_VERSION, batch_rows, sql, b->parameters, 0};
        if (!status)
            status = ud_connection_lease_read_open(b->lease, &options,
                                                   b->actual_schema ? b->actual_schema : b->schema);
    }
    if (!status && b->cursor && b->cursor->count) {
        ud_operator_options options = {.struct_size = sizeof(options),
                                       .abi_version = UD_ABI_VERSION,
                                       .kind = UD_OPERATOR_SORT,
                                       .max_batch_rows = batch_rows,
                                       .keys = b->cursor->keys,
                                       .key_count = b->cursor->count,
                                       .buffers = c->execution->buffers,
                                       .executor = c->execution->executor,
                                       .partition_count = 1,
                                       .check = ud_builtin_operator_check,
                                       .user = c->activation};
        const ud_batch *inputs[] = {b->schema};
        status = ud_operator_create(c->memory, &options, inputs, &b->source_sort);
    }
    if (!status && b->materialized) {
        for (;;) {
            ud_batch *input = NULL;
            status = ud_activation_check(c->activation);
            if (!status)
                status = ud_connection_lease_authorize(b->lease, UD_ACCESS_READ);
            if (!status)
                status = ud_builtin_source_raw(b, &input);
            if (status == UD_END) {
                status = UD_OK;
                break;
            }
            if (!status && b->source_errors) {
                ud_batch *errors = b->source_errors;
                b->source_errors = NULL;
                if (errors->rows)
                    status = ud_builtin_snapshot_store(b, errors, 1);
                else
                    ud_batch_destroy(errors);
            }
            if (!status && !input->rows) {
                ud_batch_destroy(input);
                continue;
            }
            if (!status && b->source_sort) {
                status = ud_operator_push(b->source_sort, 0, input);
                ud_batch_destroy(input);
            } else if (!status)
                status = ud_builtin_snapshot_store(b, input, 0);
            else
                ud_batch_destroy(input);
            if (status)
                break;
        }
        if (!status && b->kind == SQL_SOURCE)
            status = ud_connection_lease_read_close(b->lease);
        if (!status && b->source_sort) {
            status = ud_operator_finish_input(b->source_sort, 0);
            while (!status) {
                ud_batch *input = NULL;
                status = ud_operator_pull(b->source_sort, &input, NULL);
                if (status == UD_END) {
                    status = UD_OK;
                    break;
                }
                if (!status)
                    status = ud_builtin_snapshot_store(b, input, 0);
                else
                    ud_batch_destroy(input);
            }
        }
        if (!status && b->path) {
            uint8_t hash[32];
            uint64_t started = ud_now_ms();
            status = ud_connection_lease_authorize(b->lease, UD_ACCESS_READ);
            if (!status)
                status = ud_file_hash(c->memory, b->path, hash);
            b->file_hash_ms += ud_now_ms() - started;
            if (!status && memcmp(hash, b->file_fingerprint, 32))
                status = UD_INCOMPATIBLE;
            if (!status)
                status = ud_checkpoint_source(c->activation, b->path);
        }
        if (!status && c->execution->journal) {
            ud_batch *trailing = NULL;
            ud_status saved = ud_checkpoint_snapshot_read(c->activation, c->component_id,
                                                          b->snapshot_count, &trailing);
            ud_batch_destroy(trailing);
            if (saved != UD_END)
                status = saved ? saved : UD_INCOMPATIBLE;
        }
        uint8_t digest[32];
        char fingerprint[65];
        if (!status) {
            ud_sha256_finish(&b->snapshot_hash, digest);
            static const char hex[] = "0123456789abcdef";
            for (uint32_t j = 0; j < 32; j++) {
                fingerprint[2 * j] = hex[digest[j] >> 4];
                fingerprint[2 * j + 1] = hex[digest[j] & 15];
            }
            fingerprint[64] = 0;
            status = ud_builtin_source_sealed(b, digest);
        }
        if (!status) {
            char header[384];
            int n = snprintf(header, sizeof(header),
                             "{\"policy\":\"%s\",\"fingerprint\":\"%s\",\"batches\":%llu,\"rows\":%"
                             "llu,\"errorRows\":%llu,\"ports\":[",
                             b->source_policy, fingerprint, (unsigned long long)b->snapshot_count,
                             (unsigned long long)b->snapshot_rows,
                             (unsigned long long)b->snapshot_error_rows);
            ud_bytes document = {.ctx = c->memory};
            status = ud_bytes_put(&document, header, (size_t)n);
            for (uint64_t index = 0; !status && index < b->snapshot_count; index++) {
                if (index)
                    status = ud_bytes_put(&document, ",", 1);
                if (!status)
                    status = ud_bytes_put(&document, b->snapshot_ports[index] ? "1" : "0", 1);
            }
            if (!status)
                status = ud_bytes_put(&document, "]}", 2);
            if (!status && document.size > UINT32_MAX)
                status = UD_RESOURCE;
            if (!status)
                status = ud_checkpoint_snapshot_seal(
                    c->activation, c->component_id,
                    (ud_string){(const char *)document.data, (uint32_t)document.size});
            ud_bytes_clear(&document);
        }
    }
    if (!status && b->cursor)
        ud_cursor_reset(b->cursor);
    return status;
}

static ud_status source_read_batch(builtin *b, ud_batch **out) {
    *out = NULL;
    ud_batch *input = NULL;
    ud_status status = UD_OK;
    uint64_t started = ud_now_ms();
    b->output_port = 0;
    if (b->materialized) {
        if (b->snapshot_at == b->snapshot_count)
            return UD_END;
        b->output_port = b->snapshot_ports[b->snapshot_at];
        if (b->context.execution->journal) {
            status = ud_checkpoint_snapshot_read(b->context.activation, b->context.component_id,
                                                 b->snapshot_at, &input);
            if (status == UD_END)
                status = UD_CORRUPT;
        } else {
            ud_buffer_view *view = NULL;
            ud_buffer_block *block = b->snapshot[b->snapshot_at];
            status = ud_buffer_read(block, &view);
            if (!status) {
                input = (ud_batch *)ud_buffer_view_batch(view);
                status = ud_batch_retain(input);
            }
            ud_buffer_view_release(view);
            ud_status closed = ud_buffer_release(block);
            b->snapshot[b->snapshot_at] = NULL;
            if (!status)
                status = closed;
        }
        if (!status)
            b->snapshot_at++;
    } else
        status = ud_builtin_source_raw(b, &input);
    if (!status && !b->output_port && (b->kind == SQL_SOURCE || b->kind == REST_SOURCE)) {
        ud_batch *identified = NULL;
        status = ud_builtin_source_identity(b, input, &identified);
        input = identified;
    }
    b->snapshot_read_ms += ud_now_ms() - started;
    if (!status) {
        ud_batch *selected = NULL;
        status = ud_builtin_source_partition(b, input, &selected);
        input = selected;
    }
    if (!status && b->cursor && !b->output_port) {
        status = ud_cursor_filter(b->cursor, input, out);
        ud_batch_destroy(input);
    } else
        *out = input;
    return status;
}

ud_status ud_builtin_confirm(void *ptr) {
    builtin *b = ptr;
    if (!b->cursor)
        return UD_OK;
    uint64_t before = b->cursor->confirmed;
    UD_TRY(ud_cursor_confirm(b->cursor));
    char *json = NULL;
    uint32_t length = 0;
    ud_status status = ud_cursor_serialize(b->cursor, &json, &length);
    ud_bytes record = {.ctx = b->context.memory};
    char source[33], snapshot[65], schema[65], head[512];
    ud_id_format(b->source_scope, source);
    uint8_t schema_hash[32];
    if (!status)
        status = ud_schema_fingerprint(b->schema, schema_hash);
    if (!status) {
        static const char hex[] = "0123456789abcdef";
        for (uint32_t i = 0; i < 32; i++) {
            snapshot[i * 2] = hex[b->snapshot_fingerprint[i] >> 4];
            snapshot[i * 2 + 1] = hex[b->snapshot_fingerprint[i] & 15];
            schema[i * 2] = hex[schema_hash[i] >> 4];
            schema[i * 2 + 1] = hex[schema_hash[i] & 15];
        }
        snapshot[64] = schema[64] = 0;
        int n = snprintf(head, sizeof(head),
                         "{\"version\":%u,\"sourceIdentity\":\"%s\","
                         "\"snapshotFingerprint\":\"%s\",\"schemaFingerprint\":\"%s\","
                         "\"partition\":{\"id\":%u,\"count\":%u,\"begin\":%llu,\"end\":%llu},"
                         "\"cursor\":",
                         UD_CONFIG_VERSION, source, snapshot, schema, b->source_partition,
                         b->source_partitions, (unsigned long long)b->source_begin,
                         (unsigned long long)b->source_end);
        status = ud_bytes_put(&record, head, (size_t)n);
    }
    if (!status)
        status = ud_bytes_put(&record, json, length);
    if (!status)
        status = ud_bytes_put(&record, "}", 1);
    if (!status)
        status = ud_checkpoint_cursor(b->context.activation, b->context.component_id,
                                      (ud_string){(char *)record.data, (uint32_t)record.size});
    ud_bytes_clear(&record);
    ud_free(json);
    if (status) {
        b->cursor->confirmed = before;
        return UD_COMMIT_UNKNOWN;
    }
    return UD_OK;
}

ud_status ud_builtin_source_partition(builtin *b, ud_batch *input, ud_batch **out) {
    if (b->source_partitions == 1 && !b->source_begin && b->source_end == UINT64_MAX) {
        b->source_position += input->rows;
        *out = input;
        return UD_OK;
    }
    ud_status status =
        ud_batch_create(b->context.memory, input->schema->fields, input->columns, input->rows, out);
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        ud_row_info info;
        status = ud_batch_row_info(input, row, &info);
        if (status)
            break;
        uint64_t position = info.has_source_position ? info.source_position : b->source_position;
        if (position == UINT64_MAX) {
            status = UD_OVERFLOW;
            break;
        }
        if (position >= b->source_position)
            b->source_position = position + 1;
        if (position < b->source_begin || position >= b->source_end ||
            position % b->source_partitions != b->source_partition)
            continue;
        status = ud_batch_append_from(*out, input, row);
    }
    ud_batch_destroy(input);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}

ud_status ud_builtin_source_finish(builtin *b) {
    {
        uint8_t hash[32];
        uint64_t started = ud_now_ms();
        ud_status status = ud_file_hash(b->context.memory, b->path, hash);
        b->file_hash_ms += ud_now_ms() - started;
        if (!status && memcmp(hash, b->file_fingerprint, 32))
            status = UD_INCOMPATIBLE;
        return status ? status : ud_checkpoint_source(b->context.activation, b->path);
    }
}

ud_status ud_builtin_source_next(builtin *b, ud_batch **out) {
    *out = NULL;
    if (!b->output_pending) {
        if (b->source_errors) {
            b->output_pending = b->source_errors;
            b->source_errors = NULL;
            b->output_port = 1;
        } else
            UD_TRY(source_read_batch(b, &b->output_pending));
        b->output_at = 0;
    }
    uint32_t rows = b->output_pending->rows - b->output_at,
             limit = b->context.recommended_batch_rows
                         ? b->context.recommended_batch_rows(b->context.emit_user)
                         : b->context.max_batch_rows;
    if (!limit)
        limit = 1;
    if (rows > limit)
        rows = limit;
    ud_status status = ud_batch_seal(b->output_pending);
    if (!status)
        status = ud_batch_slice(b->context.memory, b->output_pending, b->output_at, rows, out);
    if (!status) {
        b->output_at += rows;
        if (b->output_at == b->output_pending->rows) {
            ud_batch_destroy(b->output_pending);
            b->output_pending = NULL;
            b->output_at = 0;
        }
    }
    return status;
}
