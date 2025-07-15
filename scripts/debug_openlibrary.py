#!/usr/bin/env python3
"""
Debug OpenLibrary API responses to understand data structure
"""

import requests
import json

def debug_work_details(work_key: str):
    """Debug a specific work to see its structure"""
    try:
        url = f"https://openlibrary.org{work_key}.json"
        print(f"Fetching: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"\n=== WORK DETAILS FOR {work_key} ===")
        print(f"Title: {data.get('title', 'N/A')}")
        print(f"Authors: {data.get('authors', 'N/A')}")
        print(f"First Published: {data.get('first_published', 'N/A')}")
        print(f"Languages: {data.get('languages', 'N/A')}")
        print(f"Editions: {len(data.get('editions', []))} editions available")
        
        # Check author structure
        authors = data.get('authors', [])
        if authors:
            print(f"\nAuthor details:")
            for i, author in enumerate(authors):
                print(f"  Author {i+1}: {author}")
                if isinstance(author, dict) and 'key' in author:
                    author_key = author['key']
                    print(f"    Key: {author_key}")
                    # Try to fetch author details
                    try:
                        author_url = f"https://openlibrary.org{author_key}.json"
                        author_response = requests.get(author_url, timeout=10)
                        if author_response.status_code == 200:
                            author_data = author_response.json()
                            print(f"    Name: {author_data.get('name', 'N/A')}")
                        else:
                            print(f"    Failed to fetch author details: {author_response.status_code}")
                    except Exception as e:
                        print(f"    Error fetching author: {e}")
        
        # Check editions
        editions = data.get('editions', [])
        if editions:
            print(f"\nEdition details:")
            edition_key = editions[0]  # First edition
            try:
                edition_url = f"https://openlibrary.org{edition_key}.json"
                edition_response = requests.get(edition_url, timeout=10)
                if edition_response.status_code == 200:
                    edition_data = edition_response.json()
                    print(f"  Edition title: {edition_data.get('title', 'N/A')}")
                    print(f"  ISBN: {edition_data.get('isbn_13', 'N/A')} / {edition_data.get('isbn_10', 'N/A')}")
                    print(f"  Pages: {edition_data.get('number_of_pages', 'N/A')}")
                    print(f"  Publish date: {edition_data.get('publish_date', 'N/A')}")
                    print(f"  Languages: {edition_data.get('languages', 'N/A')}")
                else:
                    print(f"  Failed to fetch edition: {edition_response.status_code}")
            except Exception as e:
                print(f"  Error fetching edition: {e}")
        
        return data
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def debug_subject_search(subject: str):
    """Debug a subject search to see what works are returned"""
    try:
        url = f"https://openlibrary.org/subjects/{subject}.json?limit=5"
        print(f"\n=== SUBJECT SEARCH: {subject} ===")
        print(f"URL: {url}")
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"Total works: {len(data.get('works', []))}")
        
        works = data.get('works', [])
        for i, work in enumerate(works[:3]):  # Show first 3
            print(f"\nWork {i+1}:")
            print(f"  Key: {work.get('key', 'N/A')}")
            print(f"  Title: {work.get('title', 'N/A')}")
            print(f"  Authors: {work.get('authors', 'N/A')}")
            
            # Debug this specific work
            debug_work_details(work.get('key', ''))
            break  # Only debug first work to avoid spam
        
    except Exception as e:
        print(f"Error fetching {subject}: {e}")

if __name__ == "__main__":
    # Debug a few subjects to see what's happening
    subjects = ['juvenile_fiction', 'early_readers']
    
    for subject in subjects:
        debug_subject_search(subject)
        print("\n" + "="*50 + "\n") 