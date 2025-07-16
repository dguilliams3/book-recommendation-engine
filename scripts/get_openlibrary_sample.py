#!/usr/bin/env python3
"""
Get OpenLibrary Sample Data - Efficient Version

Downloads and caches OpenLibrary data for testing the recommendation system.
Primarily uses subject search data to minimize API calls while still getting good quality data.
Filters for English books and samples from a broad set of subjects.
"""

import requests
import json
import csv
import re
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import time

RAW_DIR = Path("data/raw_openlibrary")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Helper: fetch and cache any JSON API call
def fetch_and_cache_json(url: str, local_path: Path, sleep: float = 0.5) -> Any:
    if local_path.exists():
        with open(local_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        print(f"  Downloading: {url}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        time.sleep(sleep)
        return data

# English filtering - simplified to work with subject search data
def is_english_book(work_data: Dict[str, Any]) -> bool:
    # Heuristic: skip if title looks non-English
    title = work_data.get('title', '').lower()
    if title and re.search(r'[\u0400-\u04FF\u3040-\u30FF\u4E00-\u9FFF]', title):
        return False
    return True

# Author extraction from subject search (this is what we have)
def extract_authors_from_subject_search(work_from_subject: Dict[str, Any]) -> List[str]:
    authors = []
    for author in work_from_subject.get('authors', []):
        if isinstance(author, dict) and 'name' in author:
            author_name = author['name']
            if author_name and author_name not in authors:
                authors.append(author_name)
    return authors

# Simplified publication year extraction
def extract_publication_year(work_data: Dict[str, Any]) -> Optional[int]:
    # Try to get from first_published
    if 'first_published' in work_data and work_data['first_published']:
        year = extract_year_from_string(str(work_data['first_published']))
        if year:
            return year
    return None

def extract_year_from_string(date_str: str) -> Optional[int]:
    if not date_str:
        return None
    patterns = [r'\b(19|20)\d{2}\b', r'\b\d{4}\b']
    for pattern in patterns:
        match = re.search(pattern, date_str)
        if match:
            year = int(match.group())
            if 1800 <= year <= 2030:
                return year
    return None

# Simplified reading level determination
def determine_reading_level(work_data: Dict[str, Any], subjects: List[str]) -> tuple[float, str]:
    all_text = ' '.join([
        str(work_data.get('title', '')),
        ' '.join(str(s) for s in subjects),
    ]).lower()
    
    if any(term in all_text for term in ['picture book', 'board book', 'baby book']):
        return 2.0, 'beginner'
    elif any(term in all_text for term in ['early reader', 'beginning reader', 'level 1', 'level 2']):
        return 3.0, 'early_elementary'
    elif any(term in all_text for term in ['chapter book', 'level 3', 'level 4']):
        return 4.0, 'early_elementary'
    elif any(term in all_text for term in ['middle grade', 'intermediate', 'level 5', 'level 6']):
        return 5.0, 'late_elementary'
    elif any(term in all_text for term in ['young adult', 'teen', 'adolescent']):
        return 7.0, 'middle_school'
    elif any(term in all_text for term in ['adult', 'mature', 'college']):
        return 10.0, 'high_school'
    elif any(term in all_text for term in ['children', 'juvenile', 'kids']):
        return 4.5, 'early_elementary'
    else:
        return 6.0, 'late_elementary'

# Simplified genre extraction
def extract_genres(work_data: Dict[str, Any], subjects: List[str]) -> List[str]:
    genres = set()
    all_subjects = ' '.join(str(s) for s in subjects).lower()
    title = work_data.get('title', '').lower()
    all_text = f"{all_subjects} {title}"
    
    if any(term in all_text for term in ['fantasy', 'magic', 'wizard', 'dragon']): genres.add('fantasy')
    if any(term in all_text for term in ['mystery', 'detective', 'crime']): genres.add('mystery')
    if any(term in all_text for term in ['adventure', 'exploration', 'journey']): genres.add('adventure')
    if any(term in all_text for term in ['humor', 'comedy', 'funny']): genres.add('humor')
    if any(term in all_text for term in ['romance', 'love', 'relationship']): genres.add('romance')
    if any(term in all_text for term in ['science fiction', 'sci-fi', 'space']): genres.add('science_fiction')
    if any(term in all_text for term in ['horror', 'scary', 'ghost']): genres.add('horror')
    if any(term in all_text for term in ['historical', 'history', 'period']): genres.add('historical_fiction')
    if any(term in all_text for term in ['biography', 'autobiography', 'memoir']): genres.add('biography')
    if any(term in all_text for term in ['science', 'scientific', 'physics', 'chemistry']): genres.add('science')
    if any(term in all_text for term in ['mathematics', 'math', 'algebra']): genres.add('mathematics')
    if any(term in all_text for term in ['geography', 'places', 'countries']): genres.add('geography')
    if any(term in all_text for term in ['animals', 'wildlife', 'nature']): genres.add('nature')
    if any(term in all_text for term in ['cooking', 'recipes', 'food']): genres.add('cooking')
    if any(term in all_text for term in ['art', 'painting', 'drawing']): genres.add('art')
    if any(term in all_text for term in ['music', 'musical', 'songs']): genres.add('music')
    if any(term in all_text for term in ['sports', 'athletics', 'games']): genres.add('sports')
    
    if not genres:
        if any(term in all_text for term in ['fiction', 'novel', 'story']): genres.add('fiction')
        elif any(term in all_text for term in ['non-fiction', 'nonfiction', 'factual']): genres.add('non_fiction')
        else: genres.add('fiction')
    
    return list(genres)

# Expanded subject list for broader coverage
SUBJECTS = [
    # Children/YA
    'juvenile_fiction', 'early_readers', 'childrens_literature', 'picture_books', 'young_adult_fiction',
    # Adult/General
    'fiction', 'science_fiction', 'fantasy', 'mystery', 'romance', 'historical_fiction', 'biography',
    'nonfiction', 'literature', 'thriller', 'horror', 'adventure', 'classic_literature', 'short_stories',
]

# Main data gathering - primarily using subject search data
def get_books_from_subject(subject: str, limit: int = 30) -> List[Dict[str, Any]]:
    books = []
    seen_keys = set()
    
    # Get subject search data
    subject_path = RAW_DIR / 'subjects' / f'{subject}.json'
    subject_url = f'https://openlibrary.org/subjects/{subject}.json?limit={limit}'
    data = fetch_and_cache_json(subject_url, subject_path)
    works = data.get('works', [])
    
    for work in works:
        work_key = work.get('key')
        if not work_key or work_key in seen_keys:
            continue
        seen_keys.add(work_key)
        
        # Extract basic info from subject search
        title = work.get('title', '')
        authors = extract_authors_from_subject_search(work)
        
        # Skip non-English books
        if not is_english_book(work):
            continue
        
        # Only make work API call if we need more data
        work_details = None
        if not authors or not title:  # Only if we're missing critical data
            work_path = RAW_DIR / 'works' / f'{work_key.replace("/works/", "")}.json'
            work_url = f'https://openlibrary.org{work_key}.json'
            work_details = fetch_and_cache_json(work_url, work_path)
            
            # Use work details to fill in missing data
            if not title:
                title = work_details.get('title', '')
            if not authors:
                # Extract authors from work details if needed
                author_refs = work_details.get('authors', [])
                for author_ref in author_refs:
                    if isinstance(author_ref, dict) and 'author' in author_ref:
                        author_data = author_ref['author']
                        if isinstance(author_data, dict) and 'name' in author_data:
                            author_name = author_data['name']
                            if author_name and author_name not in authors:
                                authors.append(author_name)
        
        # Get subjects for genre determination
        subjects = work.get('subject', [])
        if work_details:
            subjects.extend(work_details.get('subjects', []))
        
        # Compose book info
        books.append({
            'key': work_key,
            'title': title,
            'authors': authors,
            'subjects': subjects,
            'first_published': work.get('first_published') or (work_details.get('first_published') if work_details else None),
            'rating': work.get('rating', {}).get('average', 4.0) if isinstance(work.get('rating'), dict) else 4.0,
            'work_details': work_details
        })
    
    return books

def main():
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    all_books = []
    
    for subject in SUBJECTS:
        print(f"\n=== Fetching subject: {subject} ===")
        books = get_books_from_subject(subject, limit=20)
        all_books.extend(books)
        print(f"  Added {len(books)} books from {subject}")
    
    # Deduplicate by work key
    unique_books = {}
    for book in all_books:
        unique_books[book['key']] = book
    books = list(unique_books.values())
    
    print(f"\nTotal unique books: {len(books)}")
    
    # Write to CSV
    output_file = output_dir / "catalog_openlibrary_sample.csv"
    fieldnames = [
        'book_id', 'title', 'author', 'isbn', 'genre', 'difficulty_band',
        'reading_level', 'publication_year', 'page_count', 'average_rating'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, book in enumerate(books):
            book_id = f"OL{book['key'].replace('/works/', '')}"
            title = book['title']
            author = book['authors'][0] if book['authors'] else 'Unknown Author'
            publication_year = extract_publication_year(book)
            reading_level, difficulty_band = determine_reading_level(book, book['subjects'])
            genres = extract_genres(book, book['subjects'])
            
            writer.writerow({
                'book_id': book_id,
                'title': title,
                'author': author,
                'isbn': '',  # Not available in subject search
                'genre': json.dumps(genres),
                'difficulty_band': difficulty_band,
                'reading_level': reading_level,
                'publication_year': publication_year or '',  # Empty string for NULL
                'page_count': '',  # Empty string for NULL
                'average_rating': round(book['rating'], 1)
            })
    
    print(f"\nEfficient sample data created: {output_file}")
    print(f"Total unique books: {len(books)}")
    print("\nThis version:")
    print("- Primarily uses subject search data (minimal API calls)")
    print("- Only makes work API calls when missing critical data")
    print("- Still includes English filtering")
    print("- Extracts authors from subject search")
    print("- Determines genres from subjects")
    print("- Estimates reading levels")
    print("- Caches all API responses locally")
    print("\nTrade-offs:")
    print("- No ISBNs (would require edition API calls)")
    print("- Default page count of 150")
    print("- Limited description data")
    print("- But much faster and more reliable")
    print("\nTo use this data:")
    print("1. The file is ready in your data directory")
    print("2. Run: python -m src.ingestion_service.main")
    print("3. Or use: docker-compose up ingestion_service")

if __name__ == '__main__':
    main() 