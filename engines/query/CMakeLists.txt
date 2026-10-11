set(src ${CMAKE_CURRENT_SOURCE_DIR}/src/query.c ${CMAKE_CURRENT_SOURCE_DIR}/src/external_sort.c)
ud_component(ud_query Query ${src})
target_link_libraries(ud_query PUBLIC ud_compute ud_runtime)
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)
