# Native SSIS contracts and real ETL acceptance

These programs call the sole Native Data / Registry / Package / Execution / Connector implementation. Python generates independent input fixtures, launches processes and checks results. It never evaluates a production package. The Python business oracle and independently written OOXML fixtures are **not Microsoft SSIS Golden data**.

SDK 2.1.0, ABI and protocol family 11 replace protocol 10. The appended `ud_field.scalar` metadata carries the 29 logical domains. `UD_DATETIME` holds 100ns wall ticks and an explicit optional offset. Wire, Arrow, JSON and both bindings preserve those fields. Historical plugins/packages/checkpoints are rejected; their files are retained. Field logical identity, physical identity and expression lineage remain separate.

`formula.c` and the JSON frontend bind one IR in `expression.c`. Native Registry owns built-in and extension functions. `GETDATE`/`GETUTCDATE` are contextual rather than constant folded; durable deterministic plans reject them. Registered callbacks own their returned Native values; borrowed C#/Java callback batches expire when the callback returns. There is no managed expression evaluator.

Derived configuration uses one write plan:

```json
{"columns":[
  {"name":"Name","action":"replace","input":"Name","expression":"UPPER(TRIM([Name]))","errorDisposition":"redirect","truncationDisposition":"fail"},
  {"name":"OriginalLength","action":"add","expression":"LEN([Name])"}
],"includeInput":true}
```

Replace can select `inputIdentity` instead of `input`. It preserves the input field's position, name, domain and logical identity. All expressions read the original input, so `OriginalLength` observes the unmodified `Name`. Duplicate writes are rejected. Redirect produces one original error row and no partially written normal row. Ignored conversion errors yield a nullable added value or retain the original replacement; ignored truncation uses character boundaries. The shared error schema includes package/execution/task/component/column/row identities, source position and rule version. Raw Wire and typed `inputValues` require `includeInput`.

Task completion snapshots are tentative transaction facts before prepare. Database business effects, receipts, task/package completion and confirmation counters share the same Journal decision. Serializable MySQL/MariaDB writers atomically insert the receipt key before business effects; duplicate receipts are verified. No absent-key locking probe precedes the insert. Compute's opaque aggregate state pages obey both row and byte limits and do not inherit a business scalar domain.

Build with a configured PRODUCT dependency toolchain, .NET 10, Node 22+, JDK 17 and Maven:

```sh
cmake -S . -B build/product -DUD_BUILD_PROFILE=PRODUCT -DCMAKE_BUILD_TYPE=Release
cmake --build build/product --parallel 4
ctest --test-dir build/product --output-on-failure -LE 'real-integration|ssis-golden'
```

Use a dedicated, disposable MySQL 8+ database. `UD_TEST_MYSQL_CONFIG` names a private credential JSON file understood by the existing MySQL driver. Credentials never enter the package; only the authorized `file:` reference does. The test creates/truncates only its `ud_ssis_*` fixture tables, and writes receipt records under random execution IDs. Do not point it at a business database.

```sh
ctest --test-dir build/product --output-on-failure -R '^integration_ssis_mysql_50000$'
# The same public Native runners support a selected acceptance case:
python tests/ssis_parity/workflow.py \
  --runner build/product/tests/test_ssis_workflow \
  --database build/product/tests/test_ssis_database \
  --plugin build/product/plugins/ud_connector_mysql.so \
  --output /tmp/ud-ssis-acceptance --case csv-dirty-kill
```

Use the platform's `.exe`/`.dll` target paths on Windows. CTest obtains them directly from CMake. The default matrix includes CSV/XLSX clean/dirty, single connection and seven real XA connections, batch sizes 1/64/127/1024/4096/8192, configured concurrency 1/2/4/8, actual input-row and coordinator-commit process deaths, connection denial, a real conflicting key, reordered split conditions, separate-run history and changed-source refusal. Each success reads all business fields, row IDs, aggregate decimals and error input values back through the public Native Connector API. Completed same-ID resume must preserve data and Native confirmation counters. Failure cases must retain precisely the expected pre-existing rows. Compressed `*-readback.zip` files preserve the queried rows; `result.json` stores their hashes, output signature and Native metrics. Source CSV hashes match the supplied fixture manifest.

The designer's formula compilation goes through Management → isolated Worker → Native. Its split editor changes branch order and remaps existing connected output indices in one undoable graph edit; adding/removing branches also retains default/error destinations correctly. The real Chromium workflow checks compiled metadata, Unicode error spans, saved Add/Replace configuration, split priority, connected target identity, deployment, logging, debug pause and actual Worker termination/resume.

Golden comparison requires a genuine Windows SSIS capture:

```sh
python tests/ssis_parity/compare_golden.py \
  --manifest /acceptance/ssis/capture.json \
  --native /acceptance/native/observations.ndjson \
  --output /acceptance/ssis-differences.json
```

CTest accepts `UD_SSIS_GOLDEN_MANIFEST` and `UD_SSIS_NATIVE_OBSERVATIONS` references. Missing capture returns 77 (VerificationBlocked); complete-product CI rejects skipped gates. The manifest must name Microsoft SSIS/Windows, precise environment versions/LCID/code page/timezone, the actual dtexec command and checksummed `.dtsx`, observation and execution-log artifacts. Empty corpora and example manifests cannot establish GoldenVerified. Observations identify case, row, component and output field, and explicitly include value/type/null/length/precision/scale/code page/error code/error column/output port.

The existing product model's [172-row projection](../../docs/acceptance/ssis-strict-parity.md) records the remaining code gaps, including complete cast legality, locale comparison/inheritance, some type inference and proprietary transform behavior. Passing these Native checks does not certify complete SSIS parity or production release readiness.
