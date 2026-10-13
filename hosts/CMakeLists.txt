find_program(UD_DOTNET dotnet REQUIRED)
execute_process(COMMAND ${UD_DOTNET} --version OUTPUT_VARIABLE UD_DOTNET_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT UD_DOTNET_VERSION MATCHES "^10[.]")
  message(FATAL_ERROR "Product hosts require .NET SDK 10; detected ${UD_DOTNET_VERSION}")
endif()
set(UD_HOST_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/product)
if(WIN32)
  set(UD_HOST_SUFFIX .exe)
else()
  set(UD_HOST_SUFFIX "")
endif()
add_custom_target(ud_product_hosts ALL
  COMMAND ${UD_DOTNET} publish ${PROJECT_SOURCE_DIR}/products/UniversalData.CommandLine/UniversalData.CommandLine.csproj -c Release -o ${UD_HOST_OUTPUT}/cli
  COMMAND ${UD_DOTNET} publish ${PROJECT_SOURCE_DIR}/products/UniversalData.Worker/UniversalData.Worker.csproj -c Release -o ${UD_HOST_OUTPUT}/worker
  COMMAND ${UD_DOTNET} publish ${PROJECT_SOURCE_DIR}/products/UniversalData.Management/UniversalData.Management.csproj -c Release -o ${UD_HOST_OUTPUT}/management
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:ud_data> ${UD_HOST_OUTPUT}/cli/
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:ud_data> ${UD_HOST_OUTPUT}/worker/
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:ud_data> ${UD_HOST_OUTPUT}/management/
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${UD_HOST_OUTPUT}/worker ${UD_HOST_OUTPUT}/management/worker
  DEPENDS ud_data
  VERBATIM)
add_executable(ud_cli IMPORTED GLOBAL)
set_target_properties(ud_cli PROPERTIES IMPORTED_LOCATION ${UD_HOST_OUTPUT}/cli/ud_cli${UD_HOST_SUFFIX})
add_dependencies(ud_cli ud_product_hosts)
add_executable(ud_worker IMPORTED GLOBAL)
set_target_properties(ud_worker PROPERTIES IMPORTED_LOCATION ${UD_HOST_OUTPUT}/worker/ud_worker${UD_HOST_SUFFIX})
add_dependencies(ud_worker ud_product_hosts)
install(DIRECTORY ${UD_HOST_OUTPUT}/ DESTINATION ${CMAKE_INSTALL_BINDIR}/universaldata)
