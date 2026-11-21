find_package(Threads REQUIRED)
set(src ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/cdc.c ${PROJECT_SOURCE_DIR}/connectors/rest.c ${PROJECT_SOURCE_DIR}/connectors/http.c ${PROJECT_SOURCE_DIR}/connectors/http_oauth.c ${CMAKE_CURRENT_SOURCE_DIR}/templates.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/advanced.c ${PROJECT_SOURCE_DIR}/connectors/xlsx.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/relational.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/delivery.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/error.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/quality.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/cache.c ${CMAKE_CURRENT_SOURCE_DIR}/assets.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/enumerator.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/transforms.c ${CMAKE_CURRENT_SOURCE_DIR}/resources.c ${CMAKE_CURRENT_SOURCE_DIR}/child.c ${CMAKE_CURRENT_SOURCE_DIR}/project.c ${CMAKE_CURRENT_SOURCE_DIR}/model.c ${CMAKE_CURRENT_SOURCE_DIR}/runtime.c ${CMAKE_CURRENT_SOURCE_DIR}/sql_session.c ${PROJECT_SOURCE_DIR}/connectors/sql_config.c ${CMAKE_CURRENT_SOURCE_DIR}/checkpoint.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/profile.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/http.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/process.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/filesystem.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/control.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/builtins.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/binding.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/source.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/destination.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/derive.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/row_transform.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/sql_task.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/lifecycle.c ${PROJECT_SOURCE_DIR}/connectors/manager.c ${PROJECT_SOURCE_DIR}/connectors/manager_binding.c ${PROJECT_SOURCE_DIR}/connectors/manager_pool.c ${PROJECT_SOURCE_DIR}/catalog/catalog.c ${PROJECT_SOURCE_DIR}/catalog/storage.c ${PROJECT_SOURCE_DIR}/catalog/catalog_postgresql.c ${PROJECT_SOURCE_DIR}/catalog/deployment.c ${PROJECT_SOURCE_DIR}/catalog/history.c ${PROJECT_SOURCE_DIR}/catalog/scheduling.c)
if(UD_WITH_ARROW_FILES)
  list(APPEND src ${PROJECT_SOURCE_DIR}/connectors/columnar_file.cpp)
endif()
if(UD_WITH_ICEBERG)
  list(APPEND src ${PROJECT_SOURCE_DIR}/connectors/iceberg_table.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/iceberg.c)
endif()
list(APPEND src ${CMAKE_CURRENT_SOURCE_DIR}/debug.c ${CMAKE_CURRENT_SOURCE_DIR}/preview.c ${CMAKE_CURRENT_SOURCE_DIR}/file_inspection.c ${CMAKE_CURRENT_SOURCE_DIR}/definition.c ${CMAKE_CURRENT_SOURCE_DIR}/file_generation.c)
list(APPEND src ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/network.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/wmi.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/sqlserver.c)
list(APPEND src ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/column_file.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/avro.c ${PROJECT_SOURCE_DIR}/connectors/avro.c)
list(APPEND src ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline_diagnostics.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline_frontier.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline_epoch.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/kafka_stage.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline_kafka.c ${CMAKE_CURRENT_SOURCE_DIR}/kafka_checkpoint.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline_parallel.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline_lanes.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/mq_payload.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/mq_source.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/mq_destination.c ${PROJECT_SOURCE_DIR}/connectors/schema_registry.c)
if(UD_WITH_XML)
  list(APPEND src ${PROJECT_SOURCE_DIR}/connectors/xml.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/xml.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/xml.c)
endif()
list(APPEND src ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/results.c
  ${PROJECT_SOURCE_DIR}/catalog/job_calendar.c ${PROJECT_SOURCE_DIR}/catalog/dispatch.c
  ${PROJECT_SOURCE_DIR}/catalog/placement.c ${PROJECT_SOURCE_DIR}/catalog/publication.c)
ud_component(ud_package Package ${src})
ud_configure_catalog(ud_package)
target_link_libraries(ud_package PUBLIC ud_expression ud_compute ud_runtime ud_plugins Threads::Threads ZLIB::ZLIB EXPAT::EXPAT SQLite::SQLite3)
if(UD_WITH_XML)
  target_compile_definitions(ud_package PRIVATE UD_WITH_XML)
  target_include_directories(ud_package PRIVATE ${LIBXML2_INCLUDE_DIRS} ${LIBXSLT_INCLUDE_DIR})
  target_link_libraries(ud_package PUBLIC ${LIBXML2_LIBRARIES} ${LIBXSLT_LIBRARIES})
endif()
if(UD_WITH_HTTP)
  target_compile_definitions(ud_package PRIVATE UD_WITH_HTTP)
  target_link_libraries(ud_package PUBLIC CURL::libcurl)
endif()
if(UD_WITH_NETWORK)
  target_compile_definitions(ud_package PRIVATE UD_WITH_NETWORK)
endif()
if(UD_WITH_ARROW_FILES)
  target_compile_definitions(ud_package PRIVATE UD_WITH_ARROW_FILES)
  target_link_libraries(ud_package PUBLIC UDArrow::Arrow UDArrow::Parquet)
endif()
if(UD_WITH_ICEBERG)
  target_compile_features(ud_package PRIVATE cxx_std_23)
  target_compile_definitions(ud_package PRIVATE UD_WITH_ICEBERG)
  target_link_libraries(ud_package PUBLIC iceberg::iceberg_bundle_shared iceberg::iceberg_rest_shared
    PRIVATE $<BUILD_INTERFACE:nlohmann_json::nlohmann_json>)
endif()
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)

if(WIN32)
  target_link_libraries(ud_package PUBLIC wbemuuid ole32 oleaut32)
endif()
