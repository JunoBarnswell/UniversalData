# Native Catalog

The Package component owns document storage, scheduling and lease transactions. Standalone Management uses a server-local SQLite file. Cluster Management uses a PostgreSQL 17 primary reached through the deployment's fenced writer endpoint. Both providers share the same document keys, validation, revision ordering and lease rules. The Native Journal remains the authority for execution and external commit facts.

A document key consists of tenant, kind, ID and revision. Revisions use fixed-width decimal keys for the complete positive uint64 range. Each immutable row has a digest covering its key, timestamp and canonical JSON. Reads verify the stored key and digest. Package, Project, Environment and Deployment resolve exact immutable versions. `ud_catalog_query` returns bounded indexed pages with a stable ID and revision cursor.

Standalone Catalog connections require SQLite WAL, FULL synchronization and foreign keys. The database is `catalog.sqlite3` in the authorized directory. Native upgrades supported SQLite storage revisions under its writer lock, preserving the stored documents and their digests. SQLite deployments keep Catalog storage on the local server.

## Cluster transactions

Each PostgreSQL runtime write transaction advances and locks the Catalog clock before checking the current Management term and any required run or job lease. A monotonic five-second budget bounds connection setup and each runtime write transaction. Normal data reads have a five-second I/O budget per statement. Offline migration streams the snapshot with a per-statement budget and a bounded final commit.

The Catalog requires an active synchronous standby, writable primary, WAL synchronization and full-page writes. It requests synchronous replay, then checks that a currently synchronous streaming standby has replayed the WAL position observed after commit. Only then does Native confirm a write. The PostgreSQL role needs access to the replication statistics. Provisioning, promotion eligibility and fencing of a former primary belong to the deployment. [Cluster prerequisites](worker-cluster-ha.md)

An interrupted request before commit returns a failure such as `TRANSIENT`. A missing commit reply, canceled synchronous wait or unconfirmed WAL replay yields `COMMIT_UNKNOWN` and discards the connection. An explicit rejection from the database can be returned as a definite failure. Rollback after a lost reply cannot determine whether the original transaction committed.

## Resolving an operation

Execution submission and job publication have stable request IDs scoped to the authenticated tenant and subject. Their Native records retain the request digest with the accepted result. Under PostgreSQL, `ud_catalog_get_confirmed` and `ud_catalog_find_publication` observe the receipt under the Catalog lock and confirm the same transaction synchronously. A confirmed row proves acceptance and a confirmed missing row proves absence at that point. An unavailable proof leaves the outcome unresolved. Conflicting content for an existing request ID is rejected.

General Catalog queries read from the protected primary. Their returned revisions and timestamps describe the observed state. Use a confirmed read for decisions that require proof of a particular write. Execution snapshots and events preserve Native identities and sequence numbers for diagnosis. The Catalog clock, ownership record and execution lease coordinate Management. Source acknowledgements and transaction outcomes remain under the Native execution protocol. [Transactions and recovery](transactions-and-recovery.md)
