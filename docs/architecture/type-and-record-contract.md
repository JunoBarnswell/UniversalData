# Native Data and record contracts, protocol 10

C17 Data owns the value domains. `ud_type_name` and `ud_type_parse` are the single
type registry; schema JSON, expressions and generated language enums use it.

| Domain | Representation | Semantics |
| --- | --- | --- |
| UINT64 | Unsigned 64-bit integer | Exact range 0 through 18446744073709551615; checked arithmetic and casts |
| Duration | Signed 64-bit microseconds | Negative and unbounded-hour clock text; no midnight or calendar interpretation |
| Interval | Signed months, days, microseconds | Components remain independent; comparison uses their structural tuple |
| MySQL ENUM | UINT64 ordinal and ordered dictionary | Zero remains the invalid-value sentinel; empty/numeric labels remain distinct |
| MySQL SET | UINT64 bitmap and ordered dictionary | Empty set, empty labels and every selected bit remain distinct |

Calendar arithmetic does not convert months into days or days into microseconds.
Dictionary ordinals and bitmaps are domains, not quantities: arithmetic and
SUM/AVG require an explicit cast to a plain integer. Expressions can declare
dictionary metadata on literals using the same Data schema properties.

Wire carries all components and dictionary metadata. Arrow uses UINT64,
microsecond Duration, and a 16-byte `universaldata.interval.microsecond` extension;
converting the complete signed microsecond range to nanoseconds would lose range.
JSON projects UINT64, Duration and Interval as exact strings. The .NET and Java
type declarations are generated projections of the C contract.

MySQL table scans explicitly cast ENUM/SET to unsigned values. Custom SQL using
these fields must supply that numeric projection. Controlled native loading reads
dictionary values into native-owned session variables, assigns them using an
explicit unsigned cast, then clears the variables before commit. No server-selected
file is opened. Binary JSON decoding retains integer boundaries and exact opaque
Decimal lexemes; other opaque bytes retain their MySQL type/base64 representation.

`ud_value.is_absent` distinguishes an omitted CDC field from SQL NULL. Wire, row
projection, cloning and schema mapping preserve it. Partial UPDATE statements
assign only present fields. The old key and every known old field form the
preimage predicate; an omitted field cannot become a NULL assignment. Default
PostgreSQL replica identity, key-only tuples, unchanged TOAST, and MySQL
MINIMAL/NOBLOB images use this contract. JSON/Arrow batch projection and operations
which cannot represent partial images reject them explicitly.

Every row error uses `ud_record_error`, `ud_error_schema`, and `ud_error_append`.
Conversion, truncation and business constraints are record failures; execution
faults cannot be ignored or redirected. The error retains component/field IDs,
source row ID, native/provider codes, rule and policy. The optional binary input
contains either one canonical Wire record or an explicitly enabled raw record;
`inputFormat` identifies the representation.

CSV and NDJSON isolate conversion failures only after reaching a complete record
boundary. CSV quote/boundary failure, I/O interruption, cancellation, resource
exhaustion and unknown commit remain failures. Physical record positions determine
source identities and partition membership, including rejected records. Normal
and error batches use the existing Source Snapshot store with a persisted output
port; both destination paths remain subject to the execution confirmation rules.

Compilation and behavior acceptance for this implementation are pending. SQL
Source isolation, online schema rebinding and complete recovery are tracked in the existing product model; this contract
does not declare those audit items complete.
