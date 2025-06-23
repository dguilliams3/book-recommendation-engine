CREATE TABLE IF NOT EXISTS book_embeddings(
    book_id TEXT PRIMARY KEY,
    vec VECTOR(768),
    last_event UUID
);

CREATE TABLE IF NOT EXISTS student_profile_cache(
    student_id TEXT PRIMARY KEY,
    histogram JSONB,
    last_event UUID
);

ALTER TABLE student_embeddings
    ADD COLUMN IF NOT EXISTS last_event UUID;

ALTER TABLE student_similarity
    ADD COLUMN IF NOT EXISTS last_event UUID; 