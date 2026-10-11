set(src ${CMAKE_CURRENT_SOURCE_DIR}/src/etl.c)
ud_component(ud_etl ETL ${src})
target_link_libraries(ud_etl PUBLIC ud_compute ud_runtime)
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)
