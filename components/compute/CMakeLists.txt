set(src ${CMAKE_CURRENT_SOURCE_DIR}/src/partition.c ${CMAKE_CURRENT_SOURCE_DIR}/src/operator.c ${CMAKE_CURRENT_SOURCE_DIR}/src/plan.c ${CMAKE_CURRENT_SOURCE_DIR}/src/profile.c ${CMAKE_CURRENT_SOURCE_DIR}/src/collection.c ${CMAKE_CURRENT_SOURCE_DIR}/src/compute.c ${CMAKE_CURRENT_SOURCE_DIR}/src/quality.c ${CMAKE_CURRENT_SOURCE_DIR}/src/mapping.c ${CMAKE_CURRENT_SOURCE_DIR}/src/regex.c)
ud_component(ud_compute Compute ${src})
target_link_libraries(ud_compute PUBLIC ud_columnar ud_expression ud_runtime)
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)
