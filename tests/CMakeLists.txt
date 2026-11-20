if(TARGET ud_package OR TARGET ud_worker)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
endif()
if(TARGET ud_package)
  add_executable(test_streaming_protocol streaming_protocol.c)
  target_link_libraries(test_streaming_protocol PRIVATE ud_package)
  add_test(NAME native_streaming_protocol COMMAND test_streaming_protocol)
  set_tests_properties(native_streaming_protocol PROPERTIES TIMEOUT 15)
  add_executable(test_designer_preview designer_preview.c)
  target_link_libraries(test_designer_preview PRIVATE ud_package)
  add_test(NAME native_designer_readonly_preview COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/designer_preview.py $<TARGET_FILE:test_designer_preview>)
  add_executable(test_checkpoint_contract checkpoint_contract.c)
  target_link_libraries(test_checkpoint_contract PRIVATE ud_package)
  add_test(NAME native_checkpoint_contract_revision COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/checkpoint_contract.py $<TARGET_FILE:test_checkpoint_contract>)
  add_executable(test_catalog_secrets catalog_secrets.c)
  target_link_libraries(test_catalog_secrets PRIVATE ud_package)
  add_test(NAME native_catalog_secret_migration COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/catalog_secrets.py $<TARGET_FILE:test_catalog_secrets>)
  add_executable(test_transaction_facts transaction_facts.c)
  target_link_libraries(test_transaction_facts PRIVATE ud_runtime)
  add_test(NAME transaction_fact_publication COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/transaction_facts.py $<TARGET_FILE:test_transaction_facts>)
  add_executable(test_execution_identity execution_identity.c)
  target_link_libraries(test_execution_identity PRIVATE ud_package)
  add_test(NAME execution_identity COMMAND test_execution_identity)
endif()
if(TARGET ud_foundation)
  add_executable(test_foundation foundation.c)
  target_link_libraries(test_foundation PRIVATE ud_foundation)
  add_test(NAME foundation COMMAND test_foundation)
  add_executable(test_json json.c)
  target_link_libraries(test_json PRIVATE ud_foundation)
  add_test(NAME json_document COMMAND test_json)
endif()
if(TARGET ud_columnar)
  add_executable(test_schema_identity schema_identity.c)
  target_link_libraries(test_schema_identity PRIVATE ud_columnar)
  add_test(NAME schema_physical_identity_and_nested_evolution COMMAND test_schema_identity)
  add_executable(test_schema_discovery schema_discovery.c)
  target_link_libraries(test_schema_discovery PRIVATE ud_columnar)
  add_test(NAME unified_file_schema_discovery COMMAND test_schema_discovery ${CMAKE_CURRENT_BINARY_DIR}/schema-discovery-fixture.txt)
  add_executable(test_type_domains type_domains.c)
  target_link_libraries(test_type_domains PRIVATE ud_columnar)
  add_test(NAME native_type_domains_2 COMMAND test_type_domains)
  add_executable(test_record_errors record_errors.c)
  target_link_libraries(test_record_errors PRIVATE ud_columnar)
  add_test(NAME source_record_errors COMMAND test_record_errors ${CMAKE_CURRENT_BINARY_DIR}/record-errors-fixture.txt)
  add_executable(test_sql_bulk sql_bulk.c)
  target_link_libraries(test_sql_bulk PRIVATE ud_columnar)
  add_test(NAME sql_bulk_streaming COMMAND test_sql_bulk)
  add_executable(test_columnar columnar.c)
  target_link_libraries(test_columnar PRIVATE ud_columnar)
  add_test(NAME columnar COMMAND test_columnar)
  add_executable(test_types types.c)
  target_link_libraries(test_types PRIVATE ud_columnar)
  add_test(NAME extended_scalar_types COMMAND test_types)
  add_executable(test_format format.c)
  target_link_libraries(test_format PRIVATE ud_columnar)
  add_test(NAME json_nested_format COMMAND test_format ${CMAKE_CURRENT_BINARY_DIR}/format-fixture.ndjson)
endif()
if(TARGET ud_compute)
  add_executable(test_event_window event_window.c)
  target_link_libraries(test_event_window PRIVATE ud_compute)
  add_test(NAME native_event_time_windows COMMAND test_event_window)
  set_tests_properties(native_event_time_windows PROPERTIES TIMEOUT 180)
  add_executable(test_event_match event_match.c)
  target_link_libraries(test_event_match PRIVATE ud_compute)
  add_test(NAME native_event_time_dedup_and_join COMMAND test_event_match)
  add_executable(test_stream_soak stream_soak_runner.c)
  target_link_libraries(test_stream_soak PRIVATE ud_compute)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_test(NAME native_stream_soak_short COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/stream_soak.py $<TARGET_FILE:test_stream_soak> --seconds 2)
  set_tests_properties(native_stream_soak_short PROPERTIES TIMEOUT 30)
  add_executable(test_compute compute.c)
  target_link_libraries(test_compute PRIVATE ud_compute)
  add_test(NAME compute_plan COMMAND test_compute)
  add_executable(test_nested nested.c)
  target_link_libraries(test_nested PRIVATE ud_compute)
  add_test(NAME nested_columnar_arrow COMMAND test_nested)
endif()
if(TARGET ud_compute)
  add_executable(test_regex regex.c)
  target_link_libraries(test_regex PRIVATE ud_compute)
  add_test(NAME regex_pcre2 COMMAND test_regex)
  add_executable(test_mapping mapping.c)
  target_link_libraries(test_mapping PRIVATE ud_compute)
  add_test(NAME field_mapping COMMAND test_mapping)
  add_executable(test_quality quality.c)
  target_link_libraries(test_quality PRIVATE ud_compute)
  add_test(NAME quality_classification COMMAND test_quality)
endif()

if(TARGET ud_connector_sqlite)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_executable(test_connector_sqlite connector_sqlite.c)
  target_link_libraries(test_connector_sqlite PRIVATE ud_plugins SQLite::SQLite3 Threads::Threads)
  add_test(NAME sqlite_receipts_upsert COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/connector_sqlite.py $<TARGET_FILE:test_connector_sqlite> $<TARGET_FILE:ud_connector_sqlite>)
  set_tests_properties(sqlite_receipts_upsert PROPERTIES TIMEOUT 130)
endif()
if(TARGET ud_connector_postgresql AND TARGET ud_plugins)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_executable(test_postgresql_schema postgresql_schema.c)
  target_link_libraries(test_postgresql_schema PRIVATE ud_plugins)
  add_test(NAME integration_postgresql_physical_schema COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/integration_resource.py UD_TEST_POSTGRESQL_CONFIG
    $<TARGET_FILE:test_postgresql_schema> $<TARGET_FILE:ud_connector_postgresql>)
  set_tests_properties(integration_postgresql_physical_schema PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 120
    LABELS "real-integration;postgresql" RESOURCE_LOCK ud_integration_postgresql)
  add_executable(test_postgresql_cancel postgresql_cancel.c)
  target_link_libraries(test_postgresql_cancel PRIVATE ud_plugins Threads::Threads)
  find_program(UD_TEST_PG_INITDB initdb)
  find_program(UD_TEST_PG_CTL pg_ctl)
  if(UD_TEST_PG_INITDB AND UD_TEST_PG_CTL)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    add_test(NAME postgresql_cursor_cancellation COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/postgresql_cancel.py $<TARGET_FILE:test_postgresql_cancel>
      $<TARGET_FILE:ud_connector_postgresql> ${UD_TEST_PG_INITDB} ${UD_TEST_PG_CTL})
    set_tests_properties(postgresql_cursor_cancellation PROPERTIES TIMEOUT 90)
  endif()
endif()
if(TARGET ud_expression AND TARGET ud_runtime AND TARGET ud_plugins)
  add_executable(test_architecture_foundation architecture_foundation.c)
  target_link_libraries(test_architecture_foundation PRIVATE ud_expression ud_runtime ud_plugins)
  add_test(NAME architecture_foundation COMMAND test_architecture_foundation)
endif()
if(TARGET ud_expression AND TARGET ud_compute)
  add_executable(test_value_semantics value_semantics.c)
  target_link_libraries(test_value_semantics PRIVATE ud_expression ud_compute)
  add_test(NAME shared_value_semantics COMMAND test_value_semantics)
endif()
if(TARGET ud_package)
  add_executable(test_four_engines four_engines.c)
  target_link_libraries(test_four_engines PRIVATE ud_package)
  add_test(NAME four_engine_scheduling_operators COMMAND test_four_engines)
  set_tests_properties(four_engine_scheduling_operators PROPERTIES TIMEOUT 90)
  add_executable(test_infrastructure infrastructure.c)
  target_link_libraries(test_infrastructure PRIVATE ud_package)
  add_test(NAME block_store_ownership COMMAND test_infrastructure ownership)
  add_test(NAME pipeline_tree_parallelism COMMAND test_infrastructure trees)
  add_test(NAME indexed_segment_journal COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/infrastructure_journal.py $<TARGET_FILE:test_infrastructure>)
  add_test(NAME logical_durable_task_facts COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/infrastructure_facts.py $<TARGET_FILE:test_infrastructure>)
  set_tests_properties(pipeline_tree_parallelism indexed_segment_journal logical_durable_task_facts PROPERTIES TIMEOUT 180)
  add_executable(test_core_runtime core_runtime.c)
  target_link_libraries(test_core_runtime PRIVATE ud_package)
  add_test(NAME core_runtime_resources COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/core_runtime.py $<TARGET_FILE:test_core_runtime>)
  if(TARGET ud_cli)
    add_test(NAME core_pipeline_components COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/core_pipeline.py $<TARGET_FILE:ud_cli> $<TARGET_FILE:test_wire_decode>)
  endif()
  add_executable(test_package_execution package_execution.c)
  target_link_libraries(test_package_execution PRIVATE ud_package)
  add_test(NAME package_execution COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/package_execution.py $<TARGET_FILE:test_package_execution>)
endif()
if(TARGET ud_package AND TARGET ud_cli)
  add_test(NAME value_semantics_pipeline COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/value_semantics_pipeline.py $<TARGET_FILE:ud_cli>)
  set_tests_properties(value_semantics_pipeline PROPERTIES TIMEOUT 180)
endif()
if(TARGET ud_package)
  add_executable(test_next_phase_queries next_phase_queries.c)
  target_link_libraries(test_next_phase_queries PRIVATE ud_package)
  add_executable(test_package_outcome package_outcome.c)
  target_link_libraries(test_package_outcome PRIVATE ud_package)
  add_test(NAME package_failure_boundaries COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/package_failures.py $<TARGET_FILE:test_package_outcome>)
endif()
if(TARGET ud_package AND TARGET ud_connector_sqlite)
  add_executable(test_sql_command_records sql_command_records.c)
  target_link_libraries(test_sql_command_records PRIVATE ud_package SQLite::SQLite3)
  add_test(NAME sql_command_records_recovery COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/sql_command_records.py $<TARGET_FILE:test_sql_command_records> $<TARGET_FILE:ud_connector_sqlite>)
  set_tests_properties(sql_command_records_recovery PROPERTIES TIMEOUT 180)
  add_executable(test_four_engine_connections four_engine_connections.c)
  target_link_libraries(test_four_engine_connections PRIVATE ud_package)
  add_test(NAME four_engine_connections_enumerators COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/four_engine_connections.py $<TARGET_FILE:test_four_engine_connections> $<TARGET_FILE:ud_connector_sqlite>)
  set_tests_properties(four_engine_connections_enumerators PROPERTIES TIMEOUT 120)
  add_executable(test_package_sql package_sql.c)
  target_link_libraries(test_package_sql PRIVATE ud_package)
  add_test(NAME package_sql_integration COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/package_sql.py $<TARGET_FILE:test_package_sql> $<TARGET_FILE:ud_connector_sqlite>)
endif()
if(TARGET ud_columnar)
  add_executable(typed_data_runner typed_data_runner.c)
  target_link_libraries(typed_data_runner PRIVATE ud_columnar)
endif()
if(TARGET ud_package AND TARGET ud_connector_sqlite)
  add_executable(test_package_recovery package_recovery.c)
  target_link_libraries(test_package_recovery PRIVATE ud_package)
  add_test(NAME package_durable_recovery COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/package_recovery.py $<TARGET_FILE:test_package_recovery> $<TARGET_FILE:ud_connector_sqlite>)
  set_tests_properties(package_durable_recovery PROPERTIES TIMEOUT 180)
endif()
if(TARGET ud_package)
  add_executable(test_catalog_leases catalog_leases.c)
  target_link_libraries(test_catalog_leases PRIVATE ud_package)
  add_test(NAME catalog_leases_process_recovery COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/catalog_leases.py $<TARGET_FILE:test_catalog_leases>)
  set_tests_properties(catalog_leases_process_recovery PROPERTIES TIMEOUT 60)
  add_executable(test_catalog catalog.c)
  target_link_libraries(test_catalog PRIVATE ud_package)
  add_test(NAME catalog_immutable_store COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/catalog.py $<TARGET_FILE:test_catalog>)
endif()
if(TARGET ud_package)
  add_executable(test_project project.c)
  target_link_libraries(test_project PRIVATE ud_package)
  add_test(NAME project_environment_deployment COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/project.py $<TARGET_FILE:test_project>)
endif()
if(TARGET ud_package)
  add_executable(test_child_package child_package.c)
  target_link_libraries(test_child_package PRIVATE ud_package)
  add_test(NAME child_package_execution COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/child_package.py $<TARGET_FILE:test_child_package>)
  set_tests_properties(child_package_execution PROPERTIES TIMEOUT 120)
endif()
if(TARGET ud_package)
  add_executable(test_catalog_history catalog_history.c)
  target_link_libraries(test_catalog_history PRIVATE ud_package)
  add_test(NAME catalog_execution_history COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/catalog_history.py $<TARGET_FILE:test_catalog_history>)
endif()
if(TARGET ud_package)
  add_executable(test_filesystem_task filesystem_task.c)
  target_link_libraries(test_filesystem_task PRIVATE ud_package)
  add_test(NAME filesystem_task_execution COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/filesystem_task.py $<TARGET_FILE:test_filesystem_task>)
  set_tests_properties(filesystem_task_execution PROPERTIES TIMEOUT 120)
endif()
if(TARGET ud_package)
  add_executable(test_process_task process_task.c)
  target_link_libraries(test_process_task PRIVATE ud_package)
  add_test(NAME process_task_execution COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/process_task.py $<TARGET_FILE:test_process_task>)
  set_tests_properties(process_task_execution PROPERTIES TIMEOUT 120)
endif()
if(TARGET ud_package AND UD_WITH_HTTP)
  add_executable(test_http_task http_task.c)
  target_link_libraries(test_http_task PRIVATE ud_package)
  add_test(NAME http_task_execution COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/http_task.py $<TARGET_FILE:test_http_task>)
  set_tests_properties(http_task_execution PROPERTIES TIMEOUT 120)
endif()
if(TARGET ud_package AND NOT UD_WITH_HTTP)
  add_test(NAME http_disabled_before_io COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/http_disabled.py $<TARGET_FILE:test_package_outcome>)
endif()
if(TARGET ud_compute)
  add_executable(test_profile profile.c)
  target_link_libraries(test_profile PRIVATE ud_compute)
  add_test(NAME profile_statistics_kernel COMMAND test_profile)
endif()
if(TARGET ud_package)
  add_executable(test_profile_task profile_task.c)
  target_link_libraries(test_profile_task PRIVATE ud_package)
  set(profile_test_args)
  if(TARGET ud_connector_sqlite)
    list(APPEND profile_test_args $<TARGET_FILE:ud_connector_sqlite>)
  endif()
  add_test(NAME profile_task_execution COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/profile_task.py $<TARGET_FILE:test_profile_task> ${profile_test_args})
  set_tests_properties(profile_task_execution PROPERTIES TIMEOUT 120)
endif()
if(TARGET ud_data AND TARGET ud_package)
  foreach(variant task bad)
    add_library(sample_custom_${variant} MODULE ${PROJECT_SOURCE_DIR}/examples/plugins/custom_task.c)
    target_link_libraries(sample_custom_${variant} PRIVATE ud_data)
    set_target_properties(sample_custom_${variant} PROPERTIES PREFIX "")
  endforeach()
  target_compile_definitions(sample_custom_bad PRIVATE UD_BAD_COMPONENT)
  add_executable(test_custom_task custom_task.c)
  target_link_libraries(test_custom_task PRIVATE ud_data)
  add_test(NAME native_custom_task COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/custom_task.py $<TARGET_FILE:test_custom_task> $<TARGET_FILE:sample_custom_task> $<TARGET_FILE:sample_custom_bad>)
  set_tests_properties(native_custom_task PROPERTIES TIMEOUT 120)
endif()

if(TARGET ud_connector_postgresql)
  add_executable(test_postgresql_values postgresql_values.c)
  target_link_libraries(test_postgresql_values PRIVATE ud_plugins ud_columnar)
endif()
if(TARGET ud_package AND NOT WIN32)
  add_executable(test_next_phase next_phase.c)
  target_link_libraries(test_next_phase PRIVATE ud_package)
  add_test(NAME next_phase_native_contracts COMMAND test_next_phase ${CMAKE_CURRENT_BINARY_DIR})
endif()
if(TARGET ud_package)
  add_executable(test_next_phase_execution next_phase_execution.c)
  target_link_libraries(test_next_phase_execution PRIVATE ud_package)
  if(TARGET ud_connector_sqlite)
    add_test(NAME next_phase_pipeline_recovery COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/next_phase_pipeline.py $<TARGET_FILE:test_next_phase_execution> $<TARGET_FILE:ud_connector_sqlite>)
    set_tests_properties(next_phase_pipeline_recovery PROPERTIES TIMEOUT 420)
    if(WIN32)
      set_tests_properties(next_phase_pipeline_recovery PROPERTIES TIMEOUT 900 ENVIRONMENT "UD_TEST_RECOVERY_TRACE=1")
    endif()
  endif()
endif()

if(TARGET ud_connector_sqlite AND TARGET test_next_phase_execution)
  add_test(NAME lookup_sqlite_semantics COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/lookup_sqlite.py
    $<TARGET_FILE:test_next_phase_execution> $<TARGET_FILE:ud_connector_sqlite>)
  add_test(NAME ssis_native_features COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/ssis_native_features.py
    $<TARGET_FILE:test_next_phase_execution> $<TARGET_FILE:ud_connector_sqlite>)
  set_tests_properties(ssis_native_features PROPERTIES TIMEOUT 600)
endif()

if(TARGET ud_connector_sqlite AND TARGET test_next_phase_execution)
  add_test(NAME pr5_foundation COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/pr5_foundation.py $<TARGET_FILE:test_next_phase_execution> $<TARGET_FILE:ud_connector_sqlite>)
  set_tests_properties(pr5_foundation PROPERTIES TIMEOUT 300)
endif()

if(TARGET ud_compute)
  add_executable(test_advanced_native advanced_native.c)
  target_link_libraries(test_advanced_native PRIVATE ud_compute)
  add_test(NAME advanced_native_semantics COMMAND test_advanced_native)
endif()

if(TARGET ud_plugins)
  add_executable(test_advanced_sql advanced_sql.c)
  target_link_libraries(test_advanced_sql PRIVATE ud_plugins)
endif()

if(TARGET ud_package)
  add_executable(test_wire_decode wire_decode.c)
  target_link_libraries(test_wire_decode PRIVATE ud_columnar)
endif()

if(TARGET ud_plugins)
  add_executable(test_transaction_participants transaction_participants.c)
  target_link_libraries(test_transaction_participants PRIVATE ud_plugins)
  add_executable(test_advanced_cdc advanced_cdc.c)
  target_link_libraries(test_advanced_cdc PRIVATE ud_plugins)
endif()
if(TARGET test_next_phase_execution AND UD_WITH_ARROW_FILES AND UD_WITH_HTTP)
  find_program(UD_TEST_OPENSSL openssl)
  if(UD_TEST_OPENSSL)
    add_test(NAME advanced_formats_tls_rest COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/advanced_formats.py $<TARGET_FILE:test_next_phase_execution>)
    set_tests_properties(advanced_formats_tls_rest PROPERTIES TIMEOUT 180)
  endif()
endif()
if(TARGET test_next_phase_queries AND TARGET ud_connector_sqlite)
  add_test(NAME advanced_catalog_ten_table_template COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/advanced_metadata.py --query $<TARGET_FILE:test_next_phase_queries>
    --sqlite $<TARGET_FILE:ud_connector_sqlite>)
  set_tests_properties(advanced_catalog_ten_table_template PROPERTIES TIMEOUT 180)
endif()

# Advanced SQL/CDC are actual registered acceptance entries, not unregistered
if(UD_BUILD_HOSTS)
  add_test(NAME management_authenticated_native_worker COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/management_security.py ${UD_DOTNET}
    ${CMAKE_BINARY_DIR}/hosts/product/management/UniversalData.Management.dll ${CMAKE_BINARY_DIR}/hosts/product/worker/ud_worker.dll)
  set_tests_properties(management_authenticated_native_worker PROPERTIES TIMEOUT 150)
  add_test(NAME management_worker_autoscale COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/management_worker_autoscale.py ${UD_DOTNET}
    ${CMAKE_BINARY_DIR}/hosts/product/management/UniversalData.Management.dll ${CMAKE_BINARY_DIR}/hosts/product/worker/ud_worker.dll)
  set_tests_properties(management_worker_autoscale PROPERTIES TIMEOUT 120 LABELS "cluster")
  if(UD_WITH_HTTP)
    add_test(NAME management_multi_process_claim_recovery COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/management_cluster.py ${UD_DOTNET}
      ${CMAKE_BINARY_DIR}/hosts/product/management/UniversalData.Management.dll ${CMAKE_BINARY_DIR}/hosts/product/worker/ud_worker.dll)
    set_tests_properties(management_multi_process_claim_recovery PROPERTIES TIMEOUT 180)
  endif()
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND UD_WITH_HTTP)
    find_program(UD_TEST_OPENSSL openssl)
    if(UD_TEST_OPENSSL)
      add_test(NAME management_remote_worker_dispatch COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/management_remote_nodes.py ${UD_DOTNET}
        ${CMAKE_BINARY_DIR}/hosts/product/management/UniversalData.Management.dll
        ${CMAKE_BINARY_DIR}/hosts/product/worker/ud_worker.dll ${UD_TEST_OPENSSL})
      set_tests_properties(management_remote_worker_dispatch PROPERTIES TIMEOUT 120 LABELS "cluster")
      if(UD_WITH_POSTGRESQL)
        add_test(NAME management_sentinel_ha COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/management_remote_nodes.py ${UD_DOTNET}
          ${CMAKE_BINARY_DIR}/hosts/product/management/UniversalData.Management.dll
          ${CMAKE_BINARY_DIR}/hosts/product/worker/ud_worker.dll ${UD_TEST_OPENSSL} --sentinel)
        set_tests_properties(management_sentinel_ha PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 150 LABELS "cluster")
      endif()
    endif()
  endif()
  if(UD_WITH_POSTGRESQL)
    add_test(NAME management_catalog_migration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/management_catalog_migration.py ${UD_DOTNET}
      ${CMAKE_BINARY_DIR}/hosts/product/management/UniversalData.Management.dll ${CMAKE_BINARY_DIR}/hosts/product/worker/ud_worker.dll)
    set_tests_properties(management_catalog_migration PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 180 LABELS "cluster")
  endif()
  if(UD_WITH_KAFKA)
    add_test(NAME management_kafka_elastic COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/management_kafka_elastic.py)
    set_tests_properties(management_kafka_elastic PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 3600 LABELS "cluster")
  endif()
  add_custom_target(ud_hosting_contracts_test ALL
    COMMAND ${UD_DOTNET} publish ${CMAKE_CURRENT_SOURCE_DIR}/hosting_contracts/HostingContracts.csproj -c Release -o ${CMAKE_CURRENT_BINARY_DIR}/hosting-contracts VERBATIM)
  add_test(NAME hosting_identity_secret_resource_contracts COMMAND ${UD_DOTNET} ${CMAKE_CURRENT_BINARY_DIR}/hosting-contracts/HostingContracts.dll)
  add_test(NAME worker_protocol COMMAND ${UD_DOTNET} ${CMAKE_CURRENT_BINARY_DIR}/hosting-contracts/HostingContracts.dll
    ${CMAKE_BINARY_DIR}/hosts/product/worker/ud_worker.dll)
  set_tests_properties(worker_protocol PROPERTIES TIMEOUT 60)
  set_tests_properties(hosting_identity_secret_resource_contracts PROPERTIES TIMEOUT 60)
  if(NOT WIN32)
    set_property(TEST hosting_identity_secret_resource_contracts APPEND PROPERTY
      ENVIRONMENT_MODIFICATION "LD_LIBRARY_PATH=path_list_prepend:$<TARGET_FILE_DIR:ud_data>")
  endif()
  add_custom_target(ud_managed_recovery_test ALL
    COMMAND ${UD_DOTNET} publish ${CMAKE_CURRENT_SOURCE_DIR}/managed_recovery/ManagedRecovery.csproj -c Release -o ${CMAKE_CURRENT_BINARY_DIR}/managed-recovery
    VERBATIM)
  add_test(NAME managed_native_epoch_process_recovery COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/managed_recovery.py ${UD_DOTNET} ${CMAKE_CURRENT_BINARY_DIR}/managed-recovery/ManagedRecovery.dll $<TARGET_FILE:ud_data>)
  set_tests_properties(managed_native_epoch_process_recovery PROPERTIES TIMEOUT 90)
  foreach(boundary window-post window-pre)
    add_test(NAME native_window_${boundary}_process_recovery COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/managed_recovery.py ${UD_DOTNET} ${CMAKE_CURRENT_BINARY_DIR}/managed-recovery/ManagedRecovery.dll $<TARGET_FILE:ud_data> ${boundary})
    set_tests_properties(native_window_${boundary}_process_recovery PROPERTIES TIMEOUT 60)
  endforeach()
  add_test(NAME managed_product_worker_process_recovery COMMAND ${UD_DOTNET}
    ${CMAKE_CURRENT_BINARY_DIR}/hosting-contracts/HostingContracts.dll ${CMAKE_BINARY_DIR}/hosts/product/worker/ud_worker.dll
    ${CMAKE_CURRENT_BINARY_DIR}/managed-recovery/ManagedRecovery.dll)
  set_tests_properties(managed_product_worker_process_recovery PROPERTIES TIMEOUT 60)
  add_custom_target(ud_managed_product_test ALL
    COMMAND ${UD_DOTNET} publish ${CMAKE_CURRENT_SOURCE_DIR}/managed_product/ManagedProduct.csproj -c Release -o ${CMAKE_CURRENT_BINARY_DIR}/managed-product
    VERBATIM)
  # These projects share SDK project references. Publish in order so concurrent
  # MSBuild invocations cannot replace the same obj/assets files.
  add_dependencies(ud_hosting_contracts_test ud_product_hosts)
  add_dependencies(ud_managed_recovery_test ud_hosting_contracts_test)
  add_dependencies(ud_managed_product_test ud_managed_recovery_test)
  add_test(NAME managed_registry_scripts_debug_lifecycle COMMAND ${UD_DOTNET}
    ${CMAKE_CURRENT_BINARY_DIR}/managed-product/ManagedProduct.dll $<TARGET_FILE:ud_data>)
  set_tests_properties(managed_registry_scripts_debug_lifecycle PROPERTIES TIMEOUT 60)
endif()
if(TARGET ud_java)
  find_package(Java REQUIRED COMPONENTS Development Runtime)
  file(GLOB_RECURSE UD_JAVA_PRODUCT_SOURCES CONFIGURE_DEPENDS ${PROJECT_SOURCE_DIR}/bindings/java/src/main/java/*.java)
  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/java-product/JavaProduct.class
    COMMAND ${Java_JAVAC_EXECUTABLE} -encoding UTF-8 -d ${CMAKE_CURRENT_BINARY_DIR}/java-product
      ${UD_JAVA_PRODUCT_SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/JavaProduct.java
    DEPENDS ${UD_JAVA_PRODUCT_SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/JavaProduct.java VERBATIM)
  add_custom_target(ud_java_product_test ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/java-product/JavaProduct.class)
  add_test(NAME java_registry_observer_catalog_lifecycle COMMAND ${Java_JAVA_EXECUTABLE}
    -Djava.library.path=$<TARGET_FILE_DIR:ud_java> -cp ${CMAKE_CURRENT_BINARY_DIR}/java-product JavaProduct)
  set_tests_properties(java_registry_observer_catalog_lifecycle PROPERTIES TIMEOUT 60)
endif()
# Advanced SQL/CDC are actual registered acceptance entries, not unregistered
# binaries. No credential is stored in a CMake cache or echoed to CTest output.
if(TARGET test_advanced_sql AND TARGET test_advanced_cdc)
  add_executable(test_mysql_domains mysql_domains.c)
  target_link_libraries(test_mysql_domains PRIVATE ud_plugins)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  foreach(driver postgresql mysql mariadb odbc oracle_oci)
    if(TARGET ud_connector_${driver})
      string(TOUPPER "${driver}" fixture)
      foreach(suite sql cdc)
        add_test(NAME integration_${driver}_${suite} COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/integration_resource.py UD_TEST_${fixture}_CONFIG
          $<TARGET_FILE:test_advanced_${suite}> $<TARGET_FILE:ud_connector_${driver}>)
        set_tests_properties(integration_${driver}_${suite} PROPERTIES
          SKIP_RETURN_CODE 77 TIMEOUT 240 LABELS "real-integration;${driver}"
          RESOURCE_LOCK ud_integration_${driver})
      endforeach()
      add_test(NAME integration_${driver}_row_command COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/integration_resource.py UD_TEST_${fixture}_CONFIG
        $<TARGET_FILE:test_advanced_sql> $<TARGET_FILE:ud_connector_${driver}> row-command)
      set_tests_properties(integration_${driver}_row_command PROPERTIES
        SKIP_RETURN_CODE 77 TIMEOUT 240 LABELS "real-integration;row-command;${driver}"
        RESOURCE_LOCK ud_integration_${driver})
      if(driver MATCHES "^(mysql|mariadb)$")
        add_test(NAME integration_${driver}_type_domains COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/integration_resource.py UD_TEST_${fixture}_CONFIG
          $<TARGET_FILE:test_mysql_domains> $<TARGET_FILE:ud_connector_${driver}>)
        set_tests_properties(integration_${driver}_type_domains PROPERTIES
          SKIP_RETURN_CODE 77 TIMEOUT 120 LABELS "real-integration;${driver}"
          RESOURCE_LOCK ud_integration_${driver})
      endif()
      if(driver MATCHES "^(postgresql|mysql|mariadb|odbc|oracle_oci)$")
        add_test(NAME integration_${driver}_native_bulk COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/integration_resource.py UD_TEST_${fixture}_CONFIG
          $<TARGET_FILE:test_advanced_sql> $<TARGET_FILE:ud_connector_${driver}> native)
        set_tests_properties(integration_${driver}_native_bulk PROPERTIES
          SKIP_RETURN_CODE 77 TIMEOUT 240 LABELS "real-integration;native-bulk;${driver}"
          RESOURCE_LOCK ud_integration_${driver})
      endif()
      if(driver MATCHES "^(postgresql|mysql|mariadb|odbc|oracle_oci)$")
        add_test(NAME integration_${driver}_participant_recovery COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/transaction_participants.py UD_TEST_${fixture}_CONFIG
          $<TARGET_FILE:test_transaction_participants> $<TARGET_FILE:ud_connector_${driver}>)
        set_tests_properties(integration_${driver}_participant_recovery PROPERTIES
          SKIP_RETURN_CODE 77 TIMEOUT 900 LABELS "real-integration;crash-recovery;${driver}"
          RESOURCE_LOCK ud_integration_${driver})
      endif()
    endif()
  endforeach()
endif()

if(TARGET ud_columnar)
  add_executable(test_file_layout file_layout.c)
  target_link_libraries(test_file_layout PRIVATE ud_columnar)
  add_test(NAME native_enterprise_file_layout COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/file_layout.py $<TARGET_FILE:test_file_layout>)
endif()

if(TARGET ud_package)
  add_executable(test_file_generation file_generation.c)
  target_link_libraries(test_file_generation PRIVATE ud_package)
  add_test(NAME file_generation_process_recovery COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/file_generation.py $<TARGET_FILE:test_file_generation>)
endif()

if(TARGET ud_package)
  add_executable(test_product_native product_native.c)
  target_link_libraries(test_product_native PRIVATE ud_package)
  add_test(NAME native_component_product_semantics COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/component_product.py $<TARGET_FILE:test_product_native>)
  if(UD_WITH_SQLITE)
    add_test(NAME native_sql_source_record_errors COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/sql_source_records.py $<TARGET_FILE:test_product_native> $<TARGET_FILE:ud_connector_sqlite>)
  endif()
  add_test(NAME native_xlsx_preserving_append COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/xlsx_append_product.py $<TARGET_FILE:test_product_native>)
  add_test(NAME native_avro_interoperability COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/avro_product.py $<TARGET_FILE:test_product_native>)
  add_test(NAME apache_avro_native_pipeline_resolution COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/avro_apache.py
    $<TARGET_FILE:test_product_native> $<TARGET_FILE:ud_data>)
  set_tests_properties(apache_avro_native_pipeline_resolution PROPERTIES TIMEOUT 120)
  if(UD_WITH_XML)
    add_test(NAME native_xml_source_xpath_xslt_xsd COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/xml_product.py $<TARGET_FILE:test_product_native>)
    set_tests_properties(native_xml_source_xpath_xslt_xsd PROPERTIES TIMEOUT 180)
  endif()
  if(UD_WITH_NETWORK)
    add_test(NAME native_object_transfer_sequence COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/network_product.py $<TARGET_FILE:test_product_native>)
    set_tests_properties(native_object_transfer_sequence PROPERTIES TIMEOUT 180)
  endif()
endif()

if(TARGET ud_connector_odbc AND TARGET ud_connector_postgresql)
  add_test(NAME integration_mssql_postgresql_participant_recovery COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/transaction_participants.py UD_TEST_ODBC_CONFIG
    $<TARGET_FILE:test_transaction_participants> $<TARGET_FILE:ud_connector_odbc>
    UD_TEST_POSTGRESQL_CONFIG $<TARGET_FILE:ud_connector_postgresql>)
  set_tests_properties(integration_mssql_postgresql_participant_recovery PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 900
    LABELS "real-integration;crash-recovery;odbc;postgresql" RESOURCE_LOCK "ud_integration_odbc;ud_integration_postgresql")
endif()

if(TARGET ud_expression)
  add_executable(test_ssis_expression ssis_parity/expression.c)
  target_link_libraries(test_ssis_expression PRIVATE ud_expression)
  add_test(NAME ssis_native_expression_contract COMMAND test_ssis_expression ${CMAKE_CURRENT_SOURCE_DIR}/ssis_parity/fixtures/documented_cases.json)
endif()
if(TARGET ud_package)
  add_executable(test_ssis_workflow ssis_parity/runner.c)
  target_link_libraries(test_ssis_workflow PRIVATE ud_package)
  add_test(NAME ssis_row_error_atomicity_recovery COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/ssis_parity/row_semantics.py $<TARGET_FILE:test_ssis_workflow>)
  set_tests_properties(ssis_row_error_atomicity_recovery PROPERTIES TIMEOUT 180)
endif()
if(TARGET ud_package AND TARGET ud_connector_mysql)
  add_executable(test_ssis_database ssis_parity/database.c)
  target_link_libraries(test_ssis_database PRIVATE ud_plugins ud_columnar)
  add_test(NAME integration_ssis_mysql_50000 COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/ssis_parity/workflow.py --runner $<TARGET_FILE:test_ssis_workflow>
    --database $<TARGET_FILE:test_ssis_database> --plugin $<TARGET_FILE:ud_connector_mysql>
    --output ${CMAKE_CURRENT_BINARY_DIR}/ssis-parity-results)
  set_tests_properties(integration_ssis_mysql_50000 PROPERTIES SKIP_RETURN_CODE 77
    TIMEOUT 5400 LABELS "real-integration;mysql;ssis-parity;performance;recovery"
    RESOURCE_LOCK ud_integration_mysql)
endif()
if(Python3_EXECUTABLE)
  add_test(NAME ssis_microsoft_golden COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/ssis_parity/compare_golden.py)
  set_tests_properties(ssis_microsoft_golden PROPERTIES SKIP_RETURN_CODE 77
    LABELS "ssis-golden;windows-resource")
endif()
if(TARGET ud_package)
  add_executable(test_message_brokers message_brokers.c)
  target_link_libraries(test_message_brokers PRIVATE ud_package)
  foreach(broker IN ITEMS activemq_classic activemq_artemis rabbitmq rocketmq kafka)
    string(REGEX REPLACE "_(classic|artemis)$" "" driver "${broker}")
    if(TARGET ud_connector_${driver} AND Python3_Interpreter_FOUND)
      string(TOUPPER "${broker}" resource)
      add_test(NAME integration_${broker} COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/integration_resource.py "UD_TEST_${resource}_CONFIG"
        $<TARGET_FILE:test_message_brokers> $<TARGET_FILE:ud_connector_${driver}>)
      set_tests_properties(integration_${broker} PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 600 LABELS "real-integration;mq")
    endif()
  endforeach()
  if(TARGET ud_connector_kafka AND Python3_Interpreter_FOUND)
    add_test(NAME integration_kafka_schema_registry COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/integration_resource.py UD_TEST_KAFKA_REGISTRY_CONFIG
      $<TARGET_FILE:test_message_brokers> $<TARGET_FILE:ud_connector_kafka> --registry)
    set_tests_properties(integration_kafka_schema_registry PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 600
      LABELS "real-integration;mq;kafka;schema-registry")
    add_test(NAME integration_kafka_process_takeover COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/kafka_process_takeover.py
      $<TARGET_FILE:test_message_brokers> $<TARGET_FILE:ud_connector_kafka>)
    set_tests_properties(integration_kafka_process_takeover PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 240
      LABELS "real-integration;mq;kafka;recovery")
  endif()
endif()

# ABI 14: the same sealed Native batches feed SQL Source, direct SQL sessions and managed callers.
if(TARGET ud_package)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_test(NAME sql_etl_package_migration COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/package_v13_migration.py)
  set_tests_properties(sql_etl_package_migration PROPERTIES LABELS "sql-etl" TIMEOUT 20)
  if(TARGET ud_connector_sqlite)
    add_executable(test_native_sql_batch native_sql_batch.c)
    target_link_libraries(test_native_sql_batch PRIVATE ud_plugins SQLite::SQLite3)
    add_test(NAME sql_etl_native_batch COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/sql_etl_fixture.py
      --native-test $<TARGET_FILE:test_native_sql_batch>
      --plugin $<TARGET_FILE:ud_connector_sqlite>)
    set_tests_properties(sql_etl_native_batch PROPERTIES LABELS "sql-etl;sqlite" TIMEOUT 120)
    if(TARGET ud_cli)
      add_test(NAME sql_etl_streaming_and_snapshot COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/sql_streaming_etl.py
        --cli $<TARGET_FILE:ud_cli> --sqlite $<TARGET_FILE:ud_connector_sqlite>)
      set_tests_properties(sql_etl_streaming_and_snapshot PROPERTIES LABELS "sql-etl;sqlite" TIMEOUT 180)
    endif()
  endif()
  if(UD_BUILD_HOSTS)
    add_custom_target(ud_managed_sql_test ALL
      COMMAND ${UD_DOTNET} publish ${CMAKE_CURRENT_SOURCE_DIR}/managed_sql/ManagedSql.csproj -c Release
        -o ${CMAKE_CURRENT_BINARY_DIR}/managed-sql
      DEPENDS ud_managed_product_test ud_product_hosts VERBATIM)
    add_test(NAME jobs_schedule_catalog COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/jobs_schedule.py ${UD_DOTNET}
      ${PROJECT_BINARY_DIR}/hosts/product/management/UniversalData.Management.dll
      ${PROJECT_BINARY_DIR}/hosts/product/worker/ud_worker.dll)
    set_tests_properties(jobs_schedule_catalog PROPERTIES LABELS "scheduling" TIMEOUT 75)
    if(TARGET ud_connector_sqlite)
      add_test(NAME sdk_worker_process_isolation COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/sdk_worker.py ${UD_DOTNET}
        ${CMAKE_CURRENT_BINARY_DIR}/managed-sql/ManagedSql.dll $<TARGET_FILE:ud_worker>
        $<TARGET_FILE:ud_connector_sqlite>)
      set_tests_properties(sdk_worker_process_isolation PROPERTIES LABELS "sql-etl;crash-recovery" TIMEOUT 100)
      if(NOT WIN32)
        set_tests_properties(sdk_worker_process_isolation PROPERTIES
          ENVIRONMENT_MODIFICATION "LD_LIBRARY_PATH=path_list_prepend:$<TARGET_FILE_DIR:ud_data>")
      endif()
    endif()
    foreach(driver sqlite postgresql mysql mariadb odbc oracle_oci)
      if(NOT TARGET ud_connector_${driver})
        continue()
      endif()
      set(provider ${driver})
      set(resource)
      set(labels "sql-etl;${driver}")
      if(NOT driver STREQUAL "sqlite")
        string(TOUPPER "${driver}" fixture)
        set(resource --resource "UD_TEST_${fixture}_CONFIG")
        list(APPEND labels "real-integration")
      endif()
      if(driver STREQUAL "odbc")
        set(provider sqlserver)
      elseif(driver STREQUAL "oracle_oci")
        set(provider oracle)
      endif()
      add_test(NAME sql_etl_managed_${driver} COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/sql_etl_fixture.py
        --dotnet ${UD_DOTNET} --assembly ${CMAKE_CURRENT_BINARY_DIR}/managed-sql/ManagedSql.dll
        --plugin $<TARGET_FILE:ud_connector_${driver}> --provider ${provider} ${resource})
      set_tests_properties(sql_etl_managed_${driver} PROPERTIES
        LABELS "${labels}" SKIP_RETURN_CODE 77 TIMEOUT 240 RESOURCE_LOCK ud_integration_${driver}
        ENVIRONMENT "UD_WORKER_PATH=$<TARGET_FILE:ud_worker>")
      if(NOT WIN32)
        set_tests_properties(sql_etl_managed_${driver} PROPERTIES
          ENVIRONMENT_MODIFICATION "LD_LIBRARY_PATH=path_list_prepend:$<TARGET_FILE_DIR:ud_data>")
      endif()
    endforeach()
  endif()
endif()

if(WIN32 AND TARGET ud_data)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  set(runtime_plugins)
  foreach(driver sqlite postgresql odbc mysql mariadb oracle_oci)
    if(TARGET ud_connector_${driver})
      list(APPEND runtime_plugins "$<TARGET_FILE:ud_connector_${driver}>")
    endif()
  endforeach()
  set(runtime_library "$<TARGET_FILE:ud_data>")
  if(TARGET ud_cli)
    list(APPEND runtime_library "$<TARGET_FILE_DIR:ud_cli>/ud_data.dll"
      "$<TARGET_FILE_DIR:ud_worker>/ud_data.dll" "$<TARGET_FILE_DIR:ud_cli>/../management/ud_data.dll"
      "$<TARGET_FILE_DIR:ud_cli>/../management/worker/ud_data.dll")
  endif()
  get_property(windows_tests DIRECTORY PROPERTY TESTS)
  set_property(TEST ${windows_tests} APPEND PROPERTY
    ENVIRONMENT_MODIFICATION "PATH=path_list_prepend:$<TARGET_FILE_DIR:ud_data>" "PYTHONUTF8=set:1")
  add_test(NAME sql_etl_windows_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/windows_sql_runtime.py --library ${runtime_library}
    --version "${UD_SDK_VERSION}" --plugins ${runtime_plugins})
  set_tests_properties(sql_etl_windows_runtime PROPERTIES LABELS "sql-etl;windows-runtime" TIMEOUT 30)
endif()
