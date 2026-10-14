#ifndef UD_XML_CONNECTOR_INTERNAL_H
#define UD_XML_CONNECTOR_INTERNAL_H
#include "../engines/package/internal.h"
#include "ud/connection_manager.h"
#include <libxml/parser.h>
#include <libxml/xpath.h>
typedef struct ud_xml_stream ud_xml_stream;
ud_status ud_xml_stream_open(const ud_component_context *, ud_connection_lease *, const char *,
                             ud_string record_path, uint32_t maximum, ud_xml_stream **);
ud_status ud_xml_stream_next(ud_xml_stream *, ud_bytes *);
uint64_t ud_xml_stream_record(const ud_xml_stream *);
ud_string ud_xml_stream_namespace_declarations(const ud_xml_stream *);
void ud_xml_stream_close(ud_xml_stream *);
/* All documents are preflighted: bounded input/nodes/depth, no DTD, no custom
 * entities, no external loads. Foreign DOM allocations have a domain reservation. */
ud_status ud_xml_document(const ud_component_context *, const uint8_t *, uint32_t, xmlDocPtr *,
                          ud_memory_reservation **);
ud_status ud_xml_read_document(const ud_component_context *, ud_connection_lease *, uint32_t,
                               xmlDocPtr *, ud_memory_reservation **);
ud_status ud_xml_namespaces(ud_context *, xmlXPathContextPtr, const ud_json *);
ud_status ud_xml_xpath(ud_context *, xmlXPathContextPtr, ud_string, xmlXPathObjectPtr *);
#endif
