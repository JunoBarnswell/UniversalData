set(src ${CMAKE_CURRENT_SOURCE_DIR}/src/quarantine.c ${CMAKE_CURRENT_SOURCE_DIR}/src/state.c ${CMAKE_CURRENT_SOURCE_DIR}/src/runtime.c)
ud_component(ud_runtime Runtime ${src})
target_link_libraries(ud_runtime PUBLIC ud_columnar)
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)
