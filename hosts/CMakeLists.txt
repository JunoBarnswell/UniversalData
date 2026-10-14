find_program(UD_DOTNET dotnet REQUIRED)
execute_process(COMMAND ${UD_DOTNET} --version OUTPUT_VARIABLE UD_DOTNET_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT UD_DOTNET_VERSION MATCHES "^10[.]")
  message(FATAL_ERROR "Product hosts require .NET SDK 10; detected ${UD_DOTNET_VERSION}")
endif()
find_program(UD_NODE node REQUIRED)
find_program(UD_NPM NAMES npm npm.cmd REQUIRED)
execute_process(COMMAND ${UD_NODE} -p "process.versions.node.split('.')[0]" OUTPUT_VARIABLE UD_NODE_MAJOR OUTPUT_STRIP_TRAILING_WHITESPACE)
if(UD_NODE_MAJOR LESS 22)
  message(FATAL_ERROR "Product designer requires Node 22 or newer")
endif()
if(NOT EXISTS "${PROJECT_SOURCE_DIR}/products/designer/package-lock.json")
  message(FATAL_ERROR "The product designer must include its exact npm dependency lock")
endif()
set(UD_DESIGNER_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/designer)
set(UD_DESIGNER_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/designer-source)
file(MAKE_DIRECTORY ${UD_DESIGNER_SOURCE})
file(GLOB_RECURSE UD_DESIGNER_SOURCES CONFIGURE_DEPENDS ${PROJECT_SOURCE_DIR}/products/designer/src/*)
add_custom_command(OUTPUT ${UD_DESIGNER_OUTPUT}/build.stamp
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/products/designer/src ${UD_DESIGNER_SOURCE}/src
  COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/products/designer/package.json ${PROJECT_SOURCE_DIR}/products/designer/package-lock.json ${PROJECT_SOURCE_DIR}/products/designer/tsconfig.json ${PROJECT_SOURCE_DIR}/products/designer/eslint.config.js ${PROJECT_SOURCE_DIR}/products/designer/index.html ${PROJECT_SOURCE_DIR}/products/designer/vite.config.ts ${UD_DESIGNER_SOURCE}/
  COMMAND ${UD_NPM} ci --no-audit --no-fund
  COMMAND ${UD_NPM} run typecheck
  COMMAND ${UD_NPM} run lint
  COMMAND ${UD_NPM} run build -- --outDir ${UD_DESIGNER_OUTPUT}
  COMMAND ${CMAKE_COMMAND} -E touch ${UD_DESIGNER_OUTPUT}/build.stamp
  DEPENDS ${UD_DESIGNER_SOURCES} ${PROJECT_SOURCE_DIR}/products/designer/package.json ${PROJECT_SOURCE_DIR}/products/designer/package-lock.json ${PROJECT_SOURCE_DIR}/products/designer/tsconfig.json ${PROJECT_SOURCE_DIR}/products/designer/vite.config.ts ${PROJECT_SOURCE_DIR}/products/designer/eslint.config.js ${PROJECT_SOURCE_DIR}/products/designer/index.html
  WORKING_DIRECTORY ${UD_DESIGNER_SOURCE}
  VERBATIM)
add_custom_target(ud_product_designer DEPENDS ${UD_DESIGNER_OUTPUT}/build.stamp)
set(UD_HOST_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/product)
if(WIN32)
  set(UD_HOST_SUFFIX .exe)
  set(UD_HOST_NATIVE_NAME ud_data.dll)
else()
  set(UD_HOST_SUFFIX "")
  set(UD_HOST_NATIVE_NAME libud_data.so)
endif()
add_custom_target(ud_product_hosts ALL
  COMMAND ${UD_DOTNET} publish ${PROJECT_SOURCE_DIR}/products/UniversalData.CommandLine/UniversalData.CommandLine.csproj -c Release -o ${UD_HOST_OUTPUT}/cli
  COMMAND ${UD_DOTNET} publish ${PROJECT_SOURCE_DIR}/products/UniversalData.Worker/UniversalData.Worker.csproj -c Release -o ${UD_HOST_OUTPUT}/worker
  COMMAND ${UD_DOTNET} publish ${PROJECT_SOURCE_DIR}/products/UniversalData.Management/UniversalData.Management.csproj -c Release -o ${UD_HOST_OUTPUT}/management -p:DesignerAssets=${UD_DESIGNER_OUTPUT}
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:ud_data> ${UD_HOST_OUTPUT}/cli/${UD_HOST_NATIVE_NAME}
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:ud_data> ${UD_HOST_OUTPUT}/worker/${UD_HOST_NATIVE_NAME}
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:ud_data> ${UD_HOST_OUTPUT}/management/${UD_HOST_NATIVE_NAME}
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${UD_HOST_OUTPUT}/worker ${UD_HOST_OUTPUT}/management/worker
  DEPENDS ud_data ud_product_designer
  VERBATIM)
add_executable(ud_cli IMPORTED GLOBAL)
set_target_properties(ud_cli PROPERTIES IMPORTED_LOCATION ${UD_HOST_OUTPUT}/cli/ud_cli${UD_HOST_SUFFIX})
add_dependencies(ud_cli ud_product_hosts)
add_executable(ud_worker IMPORTED GLOBAL)
set_target_properties(ud_worker PROPERTIES IMPORTED_LOCATION ${UD_HOST_OUTPUT}/worker/ud_worker${UD_HOST_SUFFIX})
add_dependencies(ud_worker ud_product_hosts)
install(DIRECTORY ${UD_HOST_OUTPUT}/ DESTINATION ${CMAKE_INSTALL_BINDIR}/universaldata
  USE_SOURCE_PERMISSIONS)
