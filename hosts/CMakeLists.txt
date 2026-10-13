add_executable(ud_cli cli.c)
target_link_libraries(ud_cli PRIVATE ud_package)
add_executable(ud_worker worker.c)
target_link_libraries(ud_worker PRIVATE ud_package)
install(TARGETS ud_cli ud_worker RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

if(UNIX AND NOT APPLE)
  set_target_properties(ud_cli ud_worker PROPERTIES INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
endif()
