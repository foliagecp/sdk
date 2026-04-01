package dumper

const SchemaSQL = `
CREATE TABLE IF NOT EXISTS types (
    id   TEXT PRIMARY KEY,
    body JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS objects (
    id      TEXT PRIMARY KEY,
    type_id TEXT REFERENCES types(id) ON DELETE SET NULL,
    body    JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS type_links (
    from_type TEXT NOT NULL REFERENCES types(id) ON DELETE CASCADE,
    to_type   TEXT NOT NULL REFERENCES types(id) ON DELETE CASCADE,
    name      TEXT NOT NULL,
    body      JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (from_type, name)
);

CREATE INDEX IF NOT EXISTS idx_type_links_to ON type_links(to_type);

CREATE TABLE IF NOT EXISTS object_links (
    from_obj TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    to_obj   TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    name     TEXT NOT NULL,
    tags     TEXT[] NOT NULL DEFAULT '{}',
    body     JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (from_obj, name)
);

CREATE INDEX IF NOT EXISTS idx_object_links_to ON object_links(to_obj);
`
