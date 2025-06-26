#!/usr/bin/env python3
"""
Quick database status checker for the book recommendation engine.
Run this after starting your Docker services to see what data has been loaded.
"""

import asyncio
import asyncpg
import os
from pathlib import Path

async def check_database():
    """Check database status and show table counts."""
    
    # Database connection details
    db_url = "postgresql://books:books@localhost:5432/books"
    
    try:
        print("🔍 Connecting to database...")
        conn = await asyncpg.connect(db_url)
        print("✅ Connected successfully!")
        
        # Check if tables exist and get row counts
        tables = [
            'catalog', 'students', 'checkout', 
            'student_embeddings', 'student_similarity', 'recommendation_history'
        ]
        
        print("\n📊 Database Status:")
        print("-" * 50)
        
        for table in tables:
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                print(f"📋 {table:20} | {count:6} rows")
            except Exception as e:
                print(f"❌ {table:20} | Table does not exist")
        
        # Check for recent activity
        print("\n🕒 Recent Activity:")
        print("-" * 50)
        
        try:
            recent_checkouts = await conn.fetchval(
                "SELECT COUNT(*) FROM checkout WHERE checkout_date >= CURRENT_DATE - INTERVAL '7 days'"
            )
            print(f"📚 Recent checkouts (7 days): {recent_checkouts}")
        except:
            print("📚 Recent checkouts: Table not available")
            
        try:
            recent_recommendations = await conn.fetchval(
                "SELECT COUNT(*) FROM recommendation_history WHERE recommendation_date >= CURRENT_DATE - INTERVAL '7 days'"
            )
            print(f"🎯 Recent recommendations (7 days): {recent_recommendations}")
        except:
            print("🎯 Recent recommendations: Table not available")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print("\n💡 Make sure:")
        print("   1. Docker containers are running: docker-compose up")
        print("   2. PostgreSQL container is healthy")
        print("   3. Port 5432 is accessible")

def check_data_files():
    """Check for data files created by services."""
    
    print("\n📁 Data Files Status:")
    print("-" * 50)
    
    data_dir = Path("data")
    vector_dir = data_dir / "vector_store"
    
    # Check sample data files
    sample_files = ["catalog_sample.csv", "students_sample.csv", "checkouts_sample.csv"]
    for file in sample_files:
        if (data_dir / file).exists():
            print(f"✅ {file}")
        else:
            print(f"❌ {file}")
    
    # Check vector store files
    if vector_dir.exists():
        faiss_files = list(vector_dir.glob("*.faiss"))
        pkl_files = list(vector_dir.glob("*.pkl"))
        
        if faiss_files:
            print(f"✅ FAISS index files: {len(faiss_files)} found")
        else:
            print("❌ No FAISS index files found")
            
        if pkl_files:
            print(f"✅ FAISS metadata files: {len(pkl_files)} found")
        else:
            print("❌ No FAISS metadata files found")
    else:
        print("❌ vector_store directory not found")

if __name__ == "__main__":
    print("🔍 Book Recommendation Engine - Database Status Checker")
    print("=" * 60)
    
    check_data_files()
    
    # Try to check database
    try:
        asyncio.run(check_database())
    except KeyboardInterrupt:
        print("\n👋 Check interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    
    print("\n💡 Next steps:")
    print("   1. Run: docker-compose up")
    print("   2. Wait for services to complete")
    print("   3. Run this script again to see results") 