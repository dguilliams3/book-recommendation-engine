#!/usr/bin/env python3
"""
OpenLibrary Data Processor

Converts OpenLibrary data dumps to the catalog CSV format expected by the ingestion service.
Handles data cleaning, reading level estimation, and schema mapping.
"""

import json
import csv
import gzip
import re
from typing import Dict, List, Optional, Any
from datetime import datetime
import argparse
from pathlib import Path

# Reading level estimation (simplified)
READING_LEVEL_MAPPING = {
    'picture_books': 2.0,
    'early_readers': 3.0,
    'chapter_books': 4.0,
    'middle_grade': 5.0,
    'young_adult': 7.0,
    'adult': 10.0
}

def estimate_reading_level(book_data: Dict[str, Any]) -> float:
    """Estimate reading level based on book metadata"""
    # Check for explicit reading level indicators
    subjects = book_data.get('subjects', [])
    if isinstance(subjects, str):
        subjects = [subjects]
    
    # Look for age/grade indicators in subjects
    for subject in subjects:
        subject_lower = str(subject).lower()
        if 'picture book' in subject_lower or 'ages 3-5' in subject_lower:
            return 2.0
        elif 'early reader' in subject_lower or 'ages 6-8' in subject_lower:
            return 3.0
        elif 'chapter book' in subject_lower or 'ages 7-10' in subject_lower:
            return 4.0
        elif 'middle grade' in subject_lower or 'ages 8-12' in subject_lower:
            return 5.0
        elif 'young adult' in subject_lower or 'teen' in subject_lower:
            return 7.0
    
    # Estimate based on page count and publication year
    page_count = book_data.get('number_of_pages_median', 0)
    if page_count:
        if page_count < 50:
            return 2.0
        elif page_count < 100:
            return 3.0
        elif page_count < 200:
            return 4.0
        elif page_count < 300:
            return 5.0
        else:
            return 6.0
    
    # Default to middle elementary
    return 4.5

def extract_genres(book_data: Dict[str, Any]) -> List[str]:
    """Extract genres from OpenLibrary subjects"""
    genres = []
    subjects = book_data.get('subjects', [])
    
    if isinstance(subjects, str):
        subjects = [subjects]
    
    genre_keywords = {
        'fiction': ['fiction', 'novel', 'story'],
        'fantasy': ['fantasy', 'magic', 'wizard', 'dragon'],
        'science_fiction': ['science fiction', 'sci-fi', 'space', 'robot'],
        'mystery': ['mystery', 'detective', 'sleuth'],
        'adventure': ['adventure', 'exploration', 'journey'],
        'historical': ['historical', 'history', 'period'],
        'realistic': ['realistic', 'contemporary', 'modern'],
        'humor': ['humor', 'comedy', 'funny'],
        'horror': ['horror', 'scary', 'ghost'],
        'biography': ['biography', 'autobiography', 'memoir'],
        'science': ['science', 'scientific', 'experiment'],
        'nature': ['nature', 'animals', 'environment'],
        'art': ['art', 'drawing', 'painting'],
        'sports': ['sports', 'athletics', 'game'],
        'cooking': ['cooking', 'recipe', 'food'],
        'reference': ['reference', 'encyclopedia', 'dictionary']
    }
    
    for subject in subjects:
        subject_lower = str(subject).lower()
        for genre, keywords in genre_keywords.items():
            if any(keyword in subject_lower for keyword in keywords):
                if genre not in genres:
                    genres.append(genre)
    
    # Default to fiction if no genres found
    if not genres:
        genres = ['fiction']
    
    return genres

def clean_text(text: str) -> str:
    """Clean and normalize text fields"""
    if not text:
        return ""
    
    # Remove extra whitespace and normalize
    text = re.sub(r'\s+', ' ', str(text).strip())
    # Remove special characters that might break CSV
    text = text.replace('"', "'").replace('\n', ' ').replace('\r', ' ')
    return text

def extract_isbn(book_data: Dict[str, Any]) -> Optional[str]:
    """Extract primary ISBN from book data"""
    isbns = book_data.get('isbn_13', []) or book_data.get('isbn_10', [])
    if isinstance(isbns, list) and isbns:
        return str(isbns[0])
    elif isinstance(isbns, str):
        return isbns
    return None

def extract_author(book_data: Dict[str, Any]) -> str:
    """Extract author name from book data"""
    authors = book_data.get('authors', [])
    if isinstance(authors, list) and authors:
        # Get first author name
        author_data = authors[0]
        if isinstance(author_data, dict):
            return clean_text(author_data.get('name', 'Unknown Author'))
        else:
            return clean_text(str(author_data))
    return "Unknown Author"

def process_openlibrary_dump(input_file: str, output_file: str, max_books: int = 10000, 
                           min_rating: float = 3.0, target_audience: str = 'children') -> None:
    """
    Process OpenLibrary dump and convert to catalog CSV format
    
    Args:
        input_file: Path to OpenLibrary dump file (.txt.gz)
        output_file: Path to output catalog CSV
        max_books: Maximum number of books to process
        min_rating: Minimum rating to include (if available)
        target_audience: Target audience filter ('children', 'all')
    """
    
    processed_count = 0
    skipped_count = 0
    
    # CSV headers matching your catalog schema
    fieldnames = [
        'book_id', 'title', 'author', 'isbn', 'genre', 'difficulty_band', 
        'reading_level', 'publication_year', 'page_count', 'average_rating'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Open the dump file (handles both .gz and plain text)
        open_func = gzip.open if input_file.endswith('.gz') else open
        mode = 'rt' if input_file.endswith('.gz') else 'r'
        
        with open_func(input_file, mode, encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if processed_count >= max_books:
                    break
                
                try:
                    # Parse JSON line
                    book_data = json.loads(line.strip())
                    
                    # Skip if not a book/work
                    if 'type' in book_data and book_data['type'].get('key') != '/type/work':
                        continue
                    
                    # Filter by target audience if specified
                    if target_audience == 'children':
                        subjects = book_data.get('subjects', [])
                        if isinstance(subjects, str):
                            subjects = [subjects]
                        
                        # Skip if no children's indicators
                        children_indicators = ['children', 'juvenile', 'picture book', 'early reader']
                        if not any(indicator in str(subject).lower() for subject in subjects 
                                 for indicator in children_indicators):
                            continue
                    
                    # Extract basic fields
                    title = clean_text(book_data.get('title', ''))
                    if not title:
                        continue
                    
                    author = extract_author(book_data)
                    isbn = extract_isbn(book_data)
                    
                    # Extract genres
                    genres = extract_genres(book_data)
                    genre_json = json.dumps(genres)
                    
                    # Estimate reading level
                    reading_level = estimate_reading_level(book_data)
                    
                    # Determine difficulty band
                    if reading_level < 3.0:
                        difficulty_band = 'beginner'
                    elif reading_level < 4.5:
                        difficulty_band = 'early_elementary'
                    elif reading_level < 6.0:
                        difficulty_band = 'late_elementary'
                    elif reading_level < 8.0:
                        difficulty_band = 'middle_school'
                    else:
                        difficulty_band = 'high_school'
                    
                    # Extract publication year
                    first_published = book_data.get('first_published', '')
                    if isinstance(first_published, str):
                        year_match = re.search(r'\b(19|20)\d{2}\b', first_published)
                        publication_year = int(year_match.group()) if year_match else None
                    else:
                        publication_year = first_published
                    
                    # Extract page count
                    page_count = book_data.get('number_of_pages_median', None)
                    
                    # Extract rating (if available)
                    rating = book_data.get('rating', {}).get('average', 4.0)
                    if rating and rating < min_rating:
                        skipped_count += 1
                        continue
                    
                    # Generate book ID
                    # Extract the work ID from the OpenLibrary key (e.g., /works/OL17453W -> OL17453W)
                    work_key = book_data.get('key', '')
                    if work_key.startswith('/works/'):
                        book_id = work_key.replace('/works/', '')
                    else:
                        book_id = work_key
                    
                    # Write to CSV
                    writer.writerow({
                        'book_id': book_id,
                        'title': title,
                        'author': author,
                        'isbn': isbn or '',
                        'genre': genre_json,
                        'difficulty_band': difficulty_band,
                        'reading_level': round(reading_level, 1),
                        'publication_year': publication_year or 2000,
                        'page_count': page_count or 100,
                        'average_rating': round(rating, 1) if rating else 4.0
                    })
                    
                    processed_count += 1
                    
                    if processed_count % 1000 == 0:
                        print(f"Processed {processed_count} books...")
                
                except json.JSONDecodeError:
                    # Skip malformed JSON lines
                    continue
                except Exception as e:
                    print(f"Error processing line {line_num}: {e}")
                    continue
    
    print(f"\nProcessing complete!")
    print(f"Processed: {processed_count} books")
    print(f"Skipped (low rating): {skipped_count} books")
    print(f"Output file: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Process OpenLibrary data for book recommendation system')
    parser.add_argument('input_file', help='Path to OpenLibrary dump file (.txt.gz)')
    parser.add_argument('output_file', help='Path to output catalog CSV')
    parser.add_argument('--max-books', type=int, default=10000, help='Maximum books to process')
    parser.add_argument('--min-rating', type=float, default=3.0, help='Minimum rating to include')
    parser.add_argument('--audience', choices=['children', 'all'], default='children', 
                       help='Target audience filter')
    
    args = parser.parse_args()
    
    if not Path(args.input_file).exists():
        print(f"Error: Input file {args.input_file} not found")
        return
    
    process_openlibrary_dump(
        args.input_file, 
        args.output_file, 
        max_books=args.max_books,
        min_rating=args.min_rating,
        target_audience=args.audience
    )

if __name__ == '__main__':
    main() 