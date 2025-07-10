#!/usr/bin/env python3
"""
Temporary script to run Alembic migrations with local database settings
"""

import os
import sys
from pathlib import Path

# Set environment variables for local development
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_PORT'] = '5432'
os.environ['DB_USER'] = 'books'
os.environ['DB_PASSWORD'] = 'books'
os.environ['DB_NAME'] = 'books'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

if __name__ == "__main__":
    from alembic.config import main
    
    # Run alembic with the provided arguments
    main(argv=sys.argv[1:]) 