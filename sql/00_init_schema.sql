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
    average_rating NUMERIC(3,2)
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
-- ENRICHMENT TRACKING TABLES
-- ====================================================================

-- Book metadata enrichment tracking for continuous background processing
CREATE TABLE IF NOT EXISTS book_metadata_enrichment (
    book_id TEXT PRIMARY KEY REFERENCES catalog(book_id),
    publication_year INTEGER,
    page_count INTEGER,
    isbn TEXT,
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    enrichment_status TEXT DEFAULT 'pending',
    attempts INTEGER DEFAULT 0,
    last_attempt TIMESTAMP,
    error_message TEXT,
    priority INTEGER DEFAULT 1, -- 1=low, 2=high, 3=critical
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enrichment requests table for tracking on-demand requests
CREATE TABLE IF NOT EXISTS enrichment_requests (
    request_id TEXT PRIMARY KEY,
    book_id TEXT REFERENCES catalog(book_id),
    requester TEXT NOT NULL, -- 'user', 'worker', 'background'
    priority INTEGER DEFAULT 1,
    reason TEXT,
    status TEXT DEFAULT 'pending', -- 'pending', 'in_progress', 'completed', 'failed'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    error_message TEXT
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

-- Recommendation history for deduplication and tracking (unified for both student and reader modes)
CREATE TABLE IF NOT EXISTS recommendation_history (
    user_id TEXT NOT NULL,  -- student_id for students, UUID for readers
    book_id TEXT REFERENCES catalog(book_id),
    recommended_at TIMESTAMP DEFAULT NOW(),
    justification TEXT,
    request_id TEXT,
    algorithm_used TEXT,
    score NUMERIC(3,2) DEFAULT 1.0,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, book_id)
);

-- Create indexes for the unified table
CREATE INDEX IF NOT EXISTS idx_rec_history_user_id ON recommendation_history(user_id);
CREATE INDEX IF NOT EXISTS idx_rec_history_book_id ON recommendation_history(book_id);
CREATE INDEX IF NOT EXISTS idx_rec_history_created_at ON recommendation_history(created_at);
CREATE INDEX IF NOT EXISTS idx_rec_history_request_id ON recommendation_history(request_id);



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
CREATE INDEX IF NOT EXISTS idx_catalog_rating ON catalog(average_rating);
CREATE INDEX IF NOT EXISTS idx_students_grade ON students(grade_level);
CREATE INDEX IF NOT EXISTS idx_students_teacher ON students(homeroom_teacher);

-- Similarity lookup optimization
CREATE INDEX IF NOT EXISTS idx_similarity_score ON student_similarity(sim DESC);

-- Vector similarity indexes (HNSW is better than IVFFlat for most use cases)
CREATE INDEX IF NOT EXISTS idx_student_vec_hnsw ON student_embeddings USING hnsw (vec vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_book_vec_hnsw ON book_embeddings USING hnsw (vec vector_cosine_ops);

-- Enrichment tracking indexes
CREATE INDEX IF NOT EXISTS idx_enrichment_status ON book_metadata_enrichment(enrichment_status);
CREATE INDEX IF NOT EXISTS idx_enrichment_priority ON book_metadata_enrichment(priority);
CREATE INDEX IF NOT EXISTS idx_enrichment_attempts ON book_metadata_enrichment(attempts);
CREATE INDEX IF NOT EXISTS idx_requests_status ON enrichment_requests(status);
CREATE INDEX IF NOT EXISTS idx_requests_priority ON enrichment_requests(priority);

-- ====================================================================
-- READER MODE TABLES
-- ====================================================================

-- Public users for Reader Mode
CREATE TABLE IF NOT EXISTS public_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hash_id TEXT UNIQUE NOT NULL,  -- SHA256 hash of identifier
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Books uploaded by Reader Mode users
CREATE TABLE IF NOT EXISTS uploaded_books (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES public_users(id),
    title TEXT,
    author TEXT,
    rating SMALLINT,  -- 1-5 user rating
    notes TEXT,  -- User's personal notes
    enrichment_notes TEXT,  -- LLM enrichment process notes
    raw_payload JSON,  -- Original upload data
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- New production columns for efficient querying
    isbn VARCHAR(20),
    genre VARCHAR(100) DEFAULT 'General',
    reading_level NUMERIC(3,1) DEFAULT 5.0,
    read_date DATE,
    confidence NUMERIC(3,2) DEFAULT 0.0,  -- LLM confidence score (0-1)
    
    -- Enrichment tracking columns
    enrichment_attempts INTEGER DEFAULT 0,  -- Number of enrichment attempts made
    enrichment_status VARCHAR(20) DEFAULT 'pending'  -- pending, in_progress, enriched, failed, max_attempts_reached, duplicate
);

-- Reader Mode feedback on recommendations
CREATE TABLE IF NOT EXISTS feedback (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES public_users(id),
    book_id VARCHAR NOT NULL REFERENCES catalog(book_id),
    score SMALLINT NOT NULL,  -- +1 (thumbs up) or -1 (thumbs down)
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- New column for direct hash access
    user_hash_id VARCHAR(100)
);

-- ====================================================================
-- READER MODE PERFORMANCE INDEXES
-- ====================================================================

-- Performance indexes for recommendation queries
CREATE INDEX IF NOT EXISTS idx_uploaded_books_user_id ON uploaded_books(user_id);
CREATE INDEX IF NOT EXISTS idx_feedback_user_id ON feedback(user_id);
CREATE INDEX IF NOT EXISTS idx_uploaded_books_genre ON uploaded_books(genre);
CREATE INDEX IF NOT EXISTS idx_uploaded_books_reading_level ON uploaded_books(reading_level);
CREATE INDEX IF NOT EXISTS idx_uploaded_books_confidence ON uploaded_books(confidence);

-- Enhanced filtering indexes for deduplication
CREATE INDEX IF NOT EXISTS idx_uploaded_books_title_author ON uploaded_books(title, author);
CREATE INDEX IF NOT EXISTS idx_uploaded_books_isbn ON uploaded_books(isbn);
CREATE INDEX IF NOT EXISTS idx_uploaded_books_title_lower ON uploaded_books(LOWER(title));
CREATE INDEX IF NOT EXISTS idx_uploaded_books_author_lower ON uploaded_books(LOWER(author));

-- ====================================================================
-- ENRICHMENT TRACKING VIEWS AND FUNCTIONS
-- ====================================================================

-- View for easy querying of books needing enrichment
CREATE OR REPLACE VIEW books_needing_enrichment AS
SELECT 
    c.book_id,
    c.title,
    c.author,
    c.publication_year,
    c.page_count,
    c.isbn,
    bme.enrichment_status,
    bme.attempts,
    bme.priority,
    bme.last_attempt
FROM catalog c
LEFT JOIN book_metadata_enrichment bme ON c.book_id = bme.book_id
WHERE c.publication_year IS NULL 
   OR c.page_count IS NULL 
   OR c.isbn IS NULL 
   OR c.isbn = '';

-- Function to update enrichment status
CREATE OR REPLACE FUNCTION update_enrichment_status(
    p_book_id TEXT,
    p_status TEXT,
    p_publication_year INTEGER DEFAULT NULL,
    p_page_count INTEGER DEFAULT NULL,
    p_isbn TEXT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE book_metadata_enrichment 
    SET 
        enrichment_status = p_status,
        publication_year = COALESCE(p_publication_year, publication_year),
        page_count = COALESCE(p_page_count, page_count),
        isbn = COALESCE(p_isbn, isbn),
        attempts = attempts + 1,
        last_attempt = CURRENT_TIMESTAMP,
        error_message = p_error_message,
        updated_at = CURRENT_TIMESTAMP
    WHERE book_id = p_book_id;
    
    -- Also update the catalog table if we have new data
    IF p_publication_year IS NOT NULL THEN
        UPDATE catalog SET publication_year = p_publication_year WHERE book_id = p_book_id;
    END IF;
    
    IF p_page_count IS NOT NULL THEN
        UPDATE catalog SET page_count = p_page_count WHERE book_id = p_book_id;
    END IF;
    
    IF p_isbn IS NOT NULL THEN
        UPDATE catalog SET isbn = p_isbn WHERE book_id = p_book_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to get next batch of books for enrichment
CREATE OR REPLACE FUNCTION get_enrichment_batch(
    p_batch_size INTEGER DEFAULT 50,
    p_priority INTEGER DEFAULT 1
) RETURNS TABLE(
    book_id TEXT,
    title TEXT,
    author TEXT,
    current_publication_year INTEGER,
    current_page_count INTEGER,
    current_isbn TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.book_id,
        c.title,
        c.author,
        c.publication_year,
        c.page_count,
        c.isbn
    FROM catalog c
    LEFT JOIN book_metadata_enrichment bme ON c.book_id = bme.book_id
    WHERE (c.publication_year IS NULL OR c.page_count IS NULL OR c.isbn IS NULL OR c.isbn = '')
      AND (bme.book_id IS NULL OR bme.enrichment_status IN ('pending', 'failed'))
      AND (bme.priority IS NULL OR bme.priority <= p_priority)
    ORDER BY 
        COALESCE(bme.priority, 1) DESC,
        COALESCE(bme.attempts, 0) ASC,
        c.book_id
    LIMIT p_batch_size;
END;
$$ LANGUAGE plpgsql;

-- ====================================================================
-- COMMENTS FOR MAINTENANCE
-- ====================================================================

COMMENT ON TABLE students IS 'Core student demographics and academic data';
COMMENT ON TABLE catalog IS 'Book catalog with metadata and reading difficulty metrics';
COMMENT ON TABLE checkout IS 'Student book checkout/return history with ratings';
COMMENT ON TABLE book_metadata_enrichment IS 'Tracks enrichment status and metadata for books';
COMMENT ON TABLE enrichment_requests IS 'Tracks on-demand enrichment requests from various services';
COMMENT ON TABLE student_embeddings IS 'ML embeddings for student preference modeling (1536-dim from text-embedding-3-small)';
COMMENT ON TABLE book_embeddings IS 'ML embeddings for semantic book similarity (1536-dim from text-embedding-3-small)';
COMMENT ON TABLE student_similarity IS 'Precomputed student similarity matrix for collaborative filtering';
COMMENT ON TABLE student_profile_cache IS 'Cached student reading profiles for performance';
COMMENT ON TABLE public_users IS 'Reader Mode users - anonymous public users who upload book lists';
COMMENT ON TABLE uploaded_books IS 'Books uploaded by Reader Mode users with LLM enrichment';
COMMENT ON TABLE feedback IS 'Reader Mode feedback on recommendations';

COMMENT ON COLUMN catalog.reading_level IS 'Numeric reading level (e.g., 3.5 for mid-3rd grade level)';
COMMENT ON COLUMN checkout.checkout_id IS 'Unique identifier for tracking individual checkout events';
COMMENT ON COLUMN book_metadata_enrichment.priority IS 'Enrichment priority: 1=low (background), 2=high (worker), 3=critical (user)';
COMMENT ON COLUMN enrichment_requests.requester IS 'Source of enrichment request: user, worker, or background';
COMMENT ON VIEW books_needing_enrichment IS 'View of books that need metadata enrichment';
COMMENT ON FUNCTION update_enrichment_status IS 'Updates enrichment status and metadata for a book';
COMMENT ON FUNCTION get_enrichment_batch IS 'Gets next batch of books for enrichment processing'; 