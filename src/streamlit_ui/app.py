import os, requests, streamlit as st
from common import SettingsInstance as S
from common.logging import get_logger

logger = get_logger(__name__)

API_URL = f"http://recommendation_api:{S.api_port}/recommend"

def main():
    logger.info("Starting Streamlit UI")
    st.title("📚 Elementary School Book Recommender")
    
    # Input fields
    student_id = st.text_input("Student ID", "")
    query = st.text_input("Topic / Keyword", "adventure")
    n = st.slider("How many books?", 1, 5, 3)
    
    logger.debug("UI initialized", extra={
        "student_id": student_id,
        "query": query,
        "n": n
    })

    if st.button("Get recommendations") and student_id:
        logger.info("Recommendation request initiated", extra={
            "student_id": student_id,
            "query": query,
            "n": n,
            "api_url": API_URL
        })
        
        with st.spinner("Calling API…"):
            try:
                logger.debug("Making API request", extra={
                    "student_id": student_id,
                    "n": n,
                    "query": query
                })
                
                r = requests.post(
                    API_URL, 
                    params={"student_id": student_id, "n": n, "query": query}, 
                    timeout=30
                )
                
                if r.ok:
                    recommendations = r.json()
                    logger.info("Recommendations received successfully", extra={
                        "student_id": student_id,
                        "recommendation_count": len(recommendations),
                        "status_code": r.status_code
                    })
                    
                    for i, item in enumerate(recommendations):
                        logger.debug("Displaying recommendation", extra={
                            "student_id": student_id,
                            "recommendation_index": i,
                            "book_title": item.get("title", "Unknown"),
                            "book_id": item.get("book_id", "Unknown")
                        })
                        st.subheader(item["title"])
                        st.write(item["librarian_blurb"])
                        
                else:
                    logger.error("API request failed", extra={
                        "student_id": student_id,
                        "status_code": r.status_code,
                        "response_text": r.text
                    })
                    st.error(f"Error {r.status_code}: {r.text}")
                    
            except requests.exceptions.Timeout:
                logger.error("API request timed out", extra={
                    "student_id": student_id,
                    "timeout": 30
                })
                st.error("Request timed out. Please try again.")
                
            except requests.exceptions.ConnectionError:
                logger.error("Failed to connect to API", extra={
                    "student_id": student_id,
                    "api_url": API_URL
                })
                st.error("Failed to connect to recommendation service. Please check if the service is running.")
                
            except Exception as e:
                logger.error("Unexpected error during API request", exc_info=True, extra={
                    "student_id": student_id,
                    "error": str(e)
                })
                st.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    logger.info("Streamlit UI starting")
    try:
        main()
    except Exception as e:
        logger.error("Streamlit UI failed", exc_info=True)
        st.error("An error occurred while starting the application.")
        raise 