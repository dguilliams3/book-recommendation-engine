CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS catalog (
    book_id TEXT PRIMARY KEY,
    isbn TEXT UNIQUE,
    title TEXT NOT NULL,
    genre JSONB,
    keywords JSONB,
    description TEXT,
    page_count INT,
    publication_year INT,
    difficulty_band TEXT,
    average_student_rating REAL
);

CREATE TABLE IF NOT EXISTS students (
    student_id TEXT PRIMARY KEY,
    grade_level INT,
    age INT,
    homeroom_teacher TEXT,
    prior_year_reading_score REAL,
    lunch_period INT
);

CREATE TABLE IF NOT EXISTS checkout (
    student_id TEXT REFERENCES students(student_id),
    book_id TEXT   REFERENCES catalog(book_id),
    checkout_date DATE,
    return_date DATE,
    student_rating INT,
    PRIMARY KEY (student_id, book_id, checkout_date)
);

CREATE TABLE IF NOT EXISTS recommendation_history(
    student_id TEXT,
    book_id TEXT,
    recommended_at DATE,
    PRIMARY KEY(student_id, book_id, recommended_at)
);

-- vectors & graph (populated by nightly job)
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS student_embeddings(
    student_id TEXT PRIMARY KEY,
    vec VECTOR(768)
);
CREATE TABLE IF NOT EXISTS student_similarity(
    a TEXT,
    b TEXT,
    sim REAL,
    PRIMARY KEY(a,b)
); 