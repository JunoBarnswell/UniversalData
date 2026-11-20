CREATE TABLE IF NOT EXISTS catalog_schema(
    id integer PRIMARY KEY CHECK(id=1),
    version integer NOT NULL
);
INSERT INTO catalog_schema(id,version) VALUES(1,1) ON CONFLICT(id) DO NOTHING;

CREATE TABLE IF NOT EXISTS documents(
    tenant text COLLATE "C" NOT NULL,
    kind integer NOT NULL,
    id text COLLATE "C" NOT NULL,
    revision text COLLATE "C" NOT NULL CHECK(length(revision)=20),
    document json NOT NULL,
    digest bytea NOT NULL CHECK(length(digest)=32),
    timestamp_ms bigint NOT NULL CHECK(timestamp_ms>=0),
    PRIMARY KEY(tenant,kind,id,revision)
);
CREATE INDEX IF NOT EXISTS document_time ON documents(tenant,kind,timestamp_ms,id,revision);
CREATE INDEX IF NOT EXISTS event_task ON documents(tenant,kind,(document ->> 'taskId'),timestamp_ms,id,revision);
CREATE INDEX IF NOT EXISTS event_component ON documents(tenant,kind,(document ->> 'componentId'),timestamp_ms,id,revision);

CREATE TABLE IF NOT EXISTS catalog_clock(
    id integer PRIMARY KEY CHECK(id=1),
    milliseconds bigint NOT NULL CHECK(milliseconds>=0)
);
INSERT INTO catalog_clock VALUES(1,0) ON CONFLICT(id) DO NOTHING;
CREATE TABLE IF NOT EXISTS lease_pools(
    tenant text COLLATE "C" NOT NULL,
    pool text COLLATE "C" NOT NULL,
    capacity integer NOT NULL CHECK(capacity>0),
    PRIMARY KEY(tenant,pool)
);
CREATE TABLE IF NOT EXISTS leases(
    tenant text COLLATE "C" NOT NULL,
    kind integer NOT NULL,
    id text COLLATE "C" NOT NULL,
    owner text COLLATE "C" NOT NULL,
    pool text COLLATE "C" NOT NULL,
    generation bigint NOT NULL CHECK(generation>0),
    until_ms bigint NOT NULL CHECK(until_ms>=0),
    PRIMARY KEY(tenant,kind,id)
);
CREATE INDEX IF NOT EXISTS lease_capacity ON leases(tenant,pool,until_ms);

CREATE TABLE IF NOT EXISTS run_heads(
    tenant text COLLATE "C" NOT NULL,
    id text COLLATE "C" NOT NULL,
    revision text COLLATE "C" NOT NULL,
    state text COLLATE "C" NOT NULL,
    timestamp_ms bigint NOT NULL,
    not_before_ms bigint NOT NULL,
    root integer NOT NULL,
    PRIMARY KEY(tenant,id)
);
CREATE INDEX IF NOT EXISTS run_dispatch ON run_heads(tenant,state,root,not_before_ms,timestamp_ms,id);
