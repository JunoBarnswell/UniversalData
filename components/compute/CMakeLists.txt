set(src ${CMAKE_CURRENT_SOURCE_DIR}/src/compute.c)
ud_component(ud_compute Compute ${src})
target_link_libraries(ud_compute PUBLIC ud_columnar)
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)
