# Native XLSX dependency review

zlib 1.3.1 (zlib License), Expat 2.8.3 (MIT) are Linux system dependencies resolved by CMake. Neither owns ETL logic, schemas, persistence, transactions or execution scheduling. Source/changeset semantics remain UniversalData native C17.

Archive handling admits only Store/Deflate, single disk, nonencrypted non-ZIP64 bounded entries; no extraction. Entry names reject traversal, absolute filesystem names, NUL, backslash and drive separators. CRC and compressed/decompressed lengths must agree. XML uses custom Context-quota allocations, maximum depth 32, no DTD, no external or parameter entities; relationship TargetMode External and macro/external-link parts reject. No external URL access, Office invocation or formula evaluation exists.

Shared strings are native Buffer blocks with host memory/spill limits. Deflate and XML allocations use the Context quota. Staged writer bytes reserve the existing Buffer spill budget until the file owner publishes or cleans the stage. The supplied schema is explicit; missing cached formulas and invalid cells use declared fail/redirect/null policies.

Acceptance includes malformed ZIP/XML, resource limits, external relationships/DTD, formulas and physical row/column diagnostics. Windows is exempt for this iteration; these versions are not a claim of Windows validation.
