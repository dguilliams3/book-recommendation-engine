ALTER TABLE catalog
    ADD COLUMN IF NOT EXISTS reading_level NUMERIC(4,2);

-- Optional index for faster range queries
CREATE INDEX IF NOT EXISTS idx_catalog_reading_level ON catalog(reading_level); 