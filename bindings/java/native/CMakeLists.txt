find_package(JNI REQUIRED COMPONENTS JVM)
add_library(ud_java SHARED bridge.c)
target_include_directories(ud_java PRIVATE ${JNI_INCLUDE_DIRS})
target_link_libraries(ud_java PRIVATE ud_data)
install(TARGETS ud_java RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

if(UNIX AND NOT APPLE)
  set_target_properties(ud_java PROPERTIES INSTALL_RPATH "$ORIGIN")
endif()
