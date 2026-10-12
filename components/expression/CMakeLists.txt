set(src ${CMAKE_CURRENT_SOURCE_DIR}/src/expression.c ${CMAKE_CURRENT_SOURCE_DIR}/src/functions.c)
ud_component(ud_expression Expression ${src})
target_link_libraries(ud_expression PUBLIC ud_columnar ud_plugins)
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)
