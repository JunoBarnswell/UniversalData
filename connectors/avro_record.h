#ifndef UD_AVRO_RECORD_H
#define UD_AVRO_RECORD_H
#include "ud/data.h"

/* One binary record according to the supplied Avro JSON schemas. Schema Registry
 * framing belongs to the caller. Encode returns storage released with ud_free. */
ud_status ud_avro_record_decode(ud_context *, ud_string writer_schema, ud_string reader_schema,
                                ud_string bytes, ud_batch *target);
ud_status ud_avro_record_encode(const ud_batch *, uint32_t row, ud_string writer_schema,
                                ud_string *bytes);
#endif
