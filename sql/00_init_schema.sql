-- ====================================================================
-- COMPLETE DATABASE SCHEMA - Book Recommendation Engine
-- ====================================================================
-- This file contains the complete schema for the book recommendation system.
-- All tables, indexes, and constraints are defined here in dependency order.

-- Extension for vector operations (if using pgvector)
CREATE EXTENSION IF NOT EXISTS vector;

-- ====================================================================
-- CORE TABLES
-- ====================================================================

-- Students table - core user data
CREATE TABLE IF NOT EXISTS students (
    student_id TEXT PRIMARY KEY,
    grade_level INT,
    age INT,
    homeroom_teacher TEXT,
    prior_year_reading_score INT,
    lunch_period TEXT
);

-- Books catalog - core book data with reading level support
CREATE TABLE IF NOT EXISTS catalog (
    book_id TEXT PRIMARY KEY,
    isbn TEXT,
    title TEXT,
    author TEXT,
    genre TEXT,
    keywords TEXT,
    description TEXT,
    page_count INT,
    publication_year INT,
    difficulty_band TEXT,
    reading_level NUMERIC(4,2),          -- Added from 02_add_reading_level.sql
    average_student_rating NUMERIC(3,2)
);

-- Checkout records - includes the checkout_id column from the start
CREATE TABLE IF NOT EXISTS checkout (
    student_id TEXT REFERENCES students(student_id),
    book_id TEXT   REFERENCES catalog(book_id),
    checkout_date DATE,
    return_date DATE,
    student_rating INT,
    checkout_id TEXT,                    -- Added for tracking
    PRIMARY KEY (student_id, book_id, checkout_date)
);

-- ====================================================================
-- EMBEDDING AND ML TABLES
-- ====================================================================

-- Student embeddings for ML recommendations
-- FIXED: text-embedding-3-small produces 1536-dimensional vectors
CREATE TABLE IF NOT EXISTS student_embeddings (
    student_id TEXT PRIMARY KEY REFERENCES students(student_id),
    vec VECTOR(1536),
    last_event UUID
);

-- Book embeddings for semantic search  
-- FIXED: text-embedding-3-small produces 1536-dimensional vectors
CREATE TABLE IF NOT EXISTS book_embeddings (
    book_id TEXT PRIMARY KEY REFERENCES catalog(book_id),
    vec VECTOR(1536),
    last_event UUID
);

-- Student similarity matrix for collaborative filtering
CREATE TABLE IF NOT EXISTS student_similarity (
    a TEXT REFERENCES students(student_id),
    b TEXT REFERENCES students(student_id),
    sim REAL,
    last_event UUID,
    PRIMARY KEY (a, b)
);

-- Student profile cache for performance optimization
CREATE TABLE IF NOT EXISTS student_profile_cache (
    student_id TEXT PRIMARY KEY REFERENCES students(student_id),
    histogram JSONB,
    last_event UUID
);

-- Recommendation history for deduplication and tracking
CREATE TABLE IF NOT EXISTS recommendation_history (
    student_id TEXT REFERENCES students(student_id),
    book_id TEXT REFERENCES catalog(book_id),
    recommended_at TIMESTAMP DEFAULT NOW(),
    justification TEXT,
    PRIMARY KEY (student_id, book_id)
);

-- ====================================================================
-- MIGRATION SAFETY: Ensure existing tables have correct structure
-- ====================================================================

-- Add missing columns if they don't exist (for backwards compatibility)
ALTER TABLE student_embeddings ADD COLUMN IF NOT EXISTS last_event UUID;
ALTER TABLE book_embeddings ADD COLUMN IF NOT EXISTS last_event UUID;
ALTER TABLE student_similarity ADD COLUMN IF NOT EXISTS last_event UUID;
ALTER TABLE student_profile_cache ADD COLUMN IF NOT EXISTS last_event UUID;

-- ====================================================================
-- PERFORMANCE INDEXES
-- ====================================================================

-- Core lookup indexes
CREATE INDEX IF NOT EXISTS idx_checkout_student_id ON checkout(student_id);
CREATE INDEX IF NOT EXISTS idx_checkout_book_id ON checkout(book_id);
CREATE INDEX IF NOT EXISTS idx_checkout_checkout_id ON checkout(checkout_id);

-- Reading level range queries
CREATE INDEX IF NOT EXISTS idx_catalog_reading_level ON catalog(reading_level);

-- Performance indexes for recommendations
CREATE INDEX IF NOT EXISTS idx_catalog_genre ON catalog(genre);
CREATE INDEX IF NOT EXISTS idx_catalog_rating ON catalog(average_student_rating);
CREATE INDEX IF NOT EXISTS idx_students_grade ON students(grade_level);
CREATE INDEX IF NOT EXISTS idx_students_teacher ON students(homeroom_teacher);

-- Similarity lookup optimization
CREATE INDEX IF NOT EXISTS idx_similarity_score ON student_similarity(sim DESC);

-- Vector similarity indexes (HNSW is better than IVFFlat for most use cases)
CREATE INDEX IF NOT EXISTS idx_student_vec_hnsw ON student_embeddings USING hnsw (vec vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_book_vec_hnsw ON book_embeddings USING hnsw (vec vector_cosine_ops);

-- ====================================================================
-- COMMENTS FOR MAINTENANCE
-- ====================================================================

COMMENT ON TABLE students IS 'Core student demographics and academic data';
COMMENT ON TABLE catalog IS 'Book catalog with metadata and reading difficulty metrics';
COMMENT ON TABLE checkout IS 'Student book checkout/return history with ratings';
COMMENT ON TABLE student_embeddings IS 'ML embeddings for student preference modeling (1536-dim from text-embedding-3-small)';
COMMENT ON TABLE book_embeddings IS 'ML embeddings for semantic book similarity (1536-dim from text-embedding-3-small)';
COMMENT ON TABLE student_similarity IS 'Precomputed student similarity matrix for collaborative filtering';
COMMENT ON TABLE student_profile_cache IS 'Cached student reading profiles for performance';

COMMENT ON COLUMN catalog.reading_level IS 'Numeric reading level (e.g., 3.5 for mid-3rd grade level)';
COMMENT ON COLUMN checkout.checkout_id IS 'Unique identifier for tracking individual checkout events';
COMMENT ON COLUMN student_embeddings.last_event IS 'UUID of last event that updated this embedding';
COMMENT ON COLUMN book_embeddings.last_event IS 'UUID of last event that updated this embedding';
COMMENT ON COLUMN student_embeddings.vec IS '1536-dimensional vector from OpenAI text-embedding-3-small';
COMMENT ON COLUMN book_embeddings.vec IS '1536-dimensional vector from OpenAI text-embedding-3-small'; 