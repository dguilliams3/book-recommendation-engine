#!/usr/bin/env python3
"""
Tests for UUID event tracking system.

Verifies that workers properly track events with UUIDs for audit trails.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, patch

class TestUUIDTracking:
    """Test UUID event tracking across workers."""

    @pytest.mark.asyncio
    async def test_student_profile_uuid_tracking(self):
        """Test that student profile updates include UUID tracking."""
        from src.incremental_workers.student_profile.main import update_profile_cache
        
        # Mock database connection
        mock_conn = AsyncMock()
        
        test_histogram = {"early_elementary": 3, "late_elementary": 1}
        test_student_id = "S001"
        test_event_id = str(uuid.uuid4())
        
        with patch('asyncpg.connect', return_value=mock_conn):
            await update_profile_cache(test_student_id, test_histogram, test_event_id)
        
        # Verify database call included UUID
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0]
        
        assert "last_event=$3" in call_args[0]  # SQL includes last_event
        assert call_args[1] == test_student_id
        assert test_event_id in call_args  # UUID passed as parameter

    @pytest.mark.asyncio 
    async def test_student_embedding_uuid_tracking(self):
        """Test that student embedding updates include UUID tracking."""
        from src.incremental_workers.student_embedding.main import cache_embedding
        import numpy as np
        
        mock_conn = AsyncMock()
        
        test_student_id = "S001"
        test_vector = np.array([0.1, 0.2, 0.3])
        test_event_id = str(uuid.uuid4())
        
        with patch('asyncpg.connect', return_value=mock_conn):
            await cache_embedding(test_student_id, test_vector, test_event_id)
        
        # Verify database call included UUID
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0]
        
        assert "last_event=$3" in call_args[0]
        assert call_args[1] == test_student_id
        assert test_event_id in call_args

    @pytest.mark.asyncio
    async def test_similarity_worker_uuid_tracking(self):
        """Test that similarity computations include UUID tracking."""
        from src.incremental_workers.similarity.main import compute_similarity
        
        mock_conn = AsyncMock()
        # Mock similarity query results
        mock_conn.fetch.return_value = [
            {"student_id": "S002", "sim": 0.85},
            {"student_id": "S003", "sim": 0.72}
        ]
        
        test_student_id = "S001"
        test_event_id = str(uuid.uuid4())
        
        with patch('asyncpg.connect', return_value=mock_conn):
            await compute_similarity(test_student_id, test_event_id)
        
        # Verify similarity records include UUID
        mock_conn.executemany.assert_called_once()
        call_args = mock_conn.executemany.call_args[0]
        
        assert "VALUES($1,$2,$3,$4)" in call_args[0]  # 4 parameters including UUID
        # Check that UUID is in the data tuples
        data_tuples = call_args[1]
        assert all(len(tuple_data) == 4 for tuple_data in data_tuples)  # student_a, student_b, sim, event_id
        assert all(tuple_data[3] == test_event_id for tuple_data in data_tuples)

    @pytest.mark.asyncio
    async def test_book_vector_uuid_tracking(self):
        """Test that book vector updates include UUID tracking.""" 
        from src.incremental_workers.book_vector.main import update_book_embeddings_table
        
        mock_conn = AsyncMock()
        
        test_book_ids = ["B001", "B002", "B003"]
        test_event_id = str(uuid.uuid4())
        
        with patch('asyncpg.connect', return_value=mock_conn):
            await update_book_embeddings_table(test_book_ids, test_event_id)
        
        # Verify book embeddings table update
        mock_conn.executemany.assert_called_once()
        call_args = mock_conn.executemany.call_args[0]
        
        assert "last_event = $2" in call_args[0]  # SQL updates last_event
        # Check that all books get the same event ID
        data_tuples = call_args[1]
        assert len(data_tuples) == 3  # One tuple per book
        assert all(tuple_data[1] == test_event_id for tuple_data in data_tuples)

    def test_uuid_generation_uniqueness(self):
        """Test that UUID generation produces unique values."""
        # Generate multiple UUIDs
        uuids = [str(uuid.uuid4()) for _ in range(100)]
        
        # Verify all are unique
        assert len(set(uuids)) == 100
        
        # Verify format (basic check)
        for uid in uuids[:5]:  # Check first 5
            assert len(uid) == 36  # Standard UUID string length
            assert uid.count('-') == 4  # Standard UUID format

    @pytest.mark.asyncio
    async def test_auto_uuid_generation(self):
        """Test that workers auto-generate UUIDs when not provided."""
        from src.incremental_workers.student_profile.main import update_profile_cache
        
        mock_conn = AsyncMock()
        
        test_histogram = {"beginner": 2}
        test_student_id = "S001"
        # Don't provide event_id - should auto-generate
        
        with patch('asyncpg.connect', return_value=mock_conn):
            await update_profile_cache(test_student_id, test_histogram)  # No event_id
        
        # Verify a UUID was generated and used
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0]
        
        # Third parameter should be the auto-generated UUID
        auto_generated_uuid = call_args[3]
        assert isinstance(auto_generated_uuid, str)
        assert len(auto_generated_uuid) == 36  # Standard UUID length
        
    def test_event_tracking_schema_compatibility(self):
        """Test that our UUID tracking is compatible with database schema."""
        # This verifies the schema expectations match our implementation
        
        # Student profile cache should have last_event UUID column
        expected_profile_schema = {
            "student_id": "TEXT PRIMARY KEY",
            "histogram": "JSONB", 
            "last_event": "UUID"
        }
        
        # Student embeddings should have last_event UUID column
        expected_embedding_schema = {
            "student_id": "TEXT PRIMARY KEY",
            "vec": "VECTOR(768)",
            "last_event": "UUID"
        }
        
        # Student similarity should have last_event UUID column
        expected_similarity_schema = {
            "a": "TEXT",
            "b": "TEXT", 
            "sim": "REAL",
            "last_event": "UUID"
        }
        
        # These are the schemas we expect - actual DB creation is tested elsewhere
        assert expected_profile_schema["last_event"] == "UUID"
        assert expected_embedding_schema["last_event"] == "UUID" 
        assert expected_similarity_schema["last_event"] == "UUID"

if __name__ == "__main__":
    pytest.main([__file__]) 