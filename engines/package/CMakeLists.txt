find_package(Threads REQUIRED)
set(src ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/transforms.c ${CMAKE_CURRENT_SOURCE_DIR}/resources.c ${CMAKE_CURRENT_SOURCE_DIR}/child.c ${CMAKE_CURRENT_SOURCE_DIR}/project.c ${CMAKE_CURRENT_SOURCE_DIR}/model.c ${CMAKE_CURRENT_SOURCE_DIR}/runtime.c ${CMAKE_CURRENT_SOURCE_DIR}/checkpoint.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/profile.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/http.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/process.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/filesystem.c ${CMAKE_CURRENT_SOURCE_DIR}/../control-flow/control.c ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/pipeline.c
  ${CMAKE_CURRENT_SOURCE_DIR}/../data-flow/builtins.c ${PROJECT_SOURCE_DIR}/connectors/manager.c ${PROJECT_SOURCE_DIR}/catalog/catalog.c ${PROJECT_SOURCE_DIR}/catalog/deployment.c ${PROJECT_SOURCE_DIR}/catalog/history.c)
ud_component(ud_package Package ${src})
target_link_libraries(ud_package PUBLIC ud_expression ud_compute ud_runtime ud_plugins Threads::Threads)
if(UD_WITH_HTTP)
  target_compile_definitions(ud_package PRIVATE UD_WITH_HTTP)
  target_link_libraries(ud_package PUBLIC CURL::libcurl)
endif()
set(UD_ALL_SOURCES ${UD_ALL_SOURCES} ${src} PARENT_SCOPE)
