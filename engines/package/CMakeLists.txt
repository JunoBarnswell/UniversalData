find_package(Threads REQUIRED)
set(src ${CMAKE_CURRENT_SOURCE_DIR}/model.c ${CMAKE_CURRENT_SOURCE_DIR}/runtime.c ${CMAKE_CURRENT_SOURCE_DIR}/checkpoint.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/control.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/builtins.c ${PROJECT_SOURCE_DIR}/connectors/manager.c ${PROJECT_SOURCE_DIR}/catalog/catalog.c)
ud_component(ud_package Package ${src})
target_link_libraries(ud_package PUBLIC ud_expression ud_compute ud_runtime ud_plugins Threads::Threads)
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)
