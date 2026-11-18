#ifndef UD_KAFKA_CHECKPOINT_H
#define UD_KAFKA_CHECKPOINT_H
#include "ud/connection_manager.h"

typedef struct ud_kafka_checkpoint ud_kafka_checkpoint;

ud_status ud_kafka_checkpoint_open(ud_activation *, ud_string connection, ud_string topic,
                                     int32_t state_partition, ud_string pipeline, ud_string group,
                                     uint32_t component_count, ud_kafka_checkpoint **);
/* Manifest storage is borrowed until the next successful commit or close. */
ud_status ud_kafka_checkpoint_manifest(ud_kafka_checkpoint *, ud_string *borrowed);
ud_status ud_kafka_checkpoint_restore(ud_kafka_checkpoint *, uint32_t component, uint64_t epoch,
                                       ud_batch **);
ud_status ud_kafka_checkpoint_begin(ud_kafka_checkpoint *);
ud_connection_lease *ud_kafka_checkpoint_producer(ud_kafka_checkpoint *);
ud_status ud_kafka_checkpoint_stage(ud_kafka_checkpoint *, uint32_t component, uint64_t epoch,
                                     const ud_batch *);
ud_status ud_kafka_checkpoint_reuse(ud_kafka_checkpoint *, uint32_t component, uint64_t epoch);
ud_status ud_kafka_checkpoint_commit(ud_kafka_checkpoint *, uint64_t epoch,
                                      ud_string pipeline_manifest, const ud_message_epoch *);
void ud_kafka_checkpoint_close(ud_kafka_checkpoint *);
#endif
