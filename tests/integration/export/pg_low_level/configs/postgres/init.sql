CREATE TABLE IF NOT EXISTS vertices (
    id TEXT PRIMARY KEY,
    body JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS links (
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    name TEXT NOT NULL,
    link_type TEXT NOT NULL DEFAULT '',
    tags TEXT[] NOT NULL DEFAULT '{}',
    body JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (from_id, name)
);

CREATE INDEX IF NOT EXISTS idx_links_to ON links(to_id);
CREATE INDEX IF NOT EXISTS idx_links_type ON links(link_type);
