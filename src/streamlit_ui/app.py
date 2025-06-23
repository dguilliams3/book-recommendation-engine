import os, requests, streamlit as st
import asyncio
import json
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from common import SettingsInstance as S
from common.structured_logging import get_logger
import pandas as pd
from sqlalchemy import create_engine

logger = get_logger(__name__)

API_URL = f"http://recommendation_api:{S.api_port}/recommend"

async def get_latest_metrics():
    """Fetch the latest metrics from Kafka"""
    try:
        # Try Docker Kafka first, then localhost for local development
        kafka_servers = ["kafka:9092", "localhost:9092"]
        
        for server in kafka_servers:
            try:
                consumer = AIOKafkaConsumer(
                    "ingestion_metrics", "api_metrics",
                    bootstrap_servers=server,
                    group_id="streamlit_dashboard",
                    auto_offset_reset="latest",
                    enable_auto_commit=False,
                )
                
                await consumer.start()
                
                # Get the latest message from each topic
                metrics = []
                try:
                    # Try to get messages with a short timeout
                    messages = await asyncio.wait_for(consumer.getmany(timeout_ms=1000), timeout=2.0)
                    for topic, msgs in messages.items():
                        for msg in msgs:
                            try:
                                data = json.loads(msg.value.decode())
                                metrics.append(data)
                            except Exception as e:
                                logger.warning(f"Failed to parse metric message: {e}")
                except asyncio.TimeoutError:
                    # No messages available, that's okay
                    pass
                finally:
                    await consumer.stop()
                    
                return metrics
                
            except Exception as e:
                logger.debug(f"Failed to connect to Kafka at {server}: {e}")
                continue
                
        # If we get here, no Kafka servers worked
        logger.warning("No Kafka servers available")
        return []
        
    except Exception as e:
        logger.warning(f"Kafka not available for metrics: {e}")
        return []

def get_recommendation(student_id: str, reading_level: str, interests: str):
    """Get book recommendation from API"""
    try:
        response = requests.post(
            API_URL,
            json={
                "student_id": student_id,
                "reading_level": reading_level,
                "interests": interests
            },
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        logger.warning("API service not available")
        return {"error": "API service not running. Please start the recommendation API."}
    except requests.exceptions.Timeout:
        logger.warning("API request timed out")
        return {"error": "API request timed out. Please try again."}
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return {"error": f"API request failed: {str(e)}"}

def get_table_df(table_name):
    try:
        # Use the same DB URL as the rest of the app
        engine = create_engine(str(S.db_url).replace("+asyncpg", ""))
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 100", conn)
        return df
    except Exception as e:
        logger.error(f"Failed to fetch table {table_name}: {e}")
        return pd.DataFrame({"error": [str(e)]})

def main():
    logger.info("Starting Streamlit UI")
    st.title("📚 Elementary School Book Recommender")
    
    # Create tabs for different sections
    tab1, tab2, tab3 = st.tabs(["📖 Book Recommendations", "�� System Metrics", "🗄️ Database Explorer"])
    
    with tab1:
        st.header("📖 Book Recommendations")
        
        with st.form("recommendation_form"):
            student_id = st.text_input("Student ID", value="student_001")
            reading_level = st.selectbox("Reading Level", ["beginner", "intermediate", "advanced"])
            interests = st.text_area("Interests (comma-separated)", value="adventure, animals, space")
            
            submitted = st.form_submit_button("Get Recommendation")
            
            if submitted:
                with st.spinner("Getting recommendation..."):
                    result = get_recommendation(student_id, reading_level, interests)
                    
                    if "error" in result:
                        st.error(result["error"])
                    else:
                        st.success("Recommendation received!")
                        st.json(result)
    
    with tab2:
        st.header("📊 System Metrics")
        
        # Check system status
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔍 System Status")
            
            # Try to connect to Kafka
            try:
                import nest_asyncio
                nest_asyncio.apply()
                metrics = asyncio.get_event_loop().run_until_complete(get_latest_metrics())
                if metrics is not None:
                    st.success("✅ Kafka Connected")
                else:
                    st.warning("⚠️ Kafka Not Available")
            except Exception as e:
                st.error(f"❌ Kafka Error: {str(e)}")
            
            # Try to connect to API
            try:
                response = requests.get(f"http://recommendation_api:{S.api_port}/health", timeout=5)
                if response.status_code == 200:
                    st.success("✅ API Connected")
                else:
                    st.warning("⚠️ API Responding but not healthy")
            except:
                st.error("❌ API Not Available")
        
        with col2:
            st.subheader("🔄 Refresh")
            if st.button("Refresh Metrics"):
                st.rerun()
        
        # Show recent metrics
        st.subheader("📈 Recent Activity")
        
        try:
            import nest_asyncio
            nest_asyncio.apply()
            metrics = asyncio.get_event_loop().run_until_complete(get_latest_metrics())
            
            if metrics:
                for metric in metrics[-5:]:  # Show last 5 metrics
                    if metric.get("event") == "ingestion_complete":
                        st.info(f"""
                        **Ingestion Complete** ({metric.get('timestamp', 'N/A')})
                        - Duration: {metric.get('duration', 'N/A')} seconds
                        - Books: {metric.get('books_processed', 'N/A')}
                        - Students: {metric.get('students_processed', 'N/A')}
                        - Checkouts: {metric.get('checkouts_processed', 'N/A')}
                        """)
                    elif metric.get("event") == "api_request":
                        st.success(f"""
                        **API Request** ({metric.get('timestamp', 'N/A')})
                        - Endpoint: {metric.get('endpoint', 'N/A')}
                        - Duration: {metric.get('duration', 'N/A')}ms
                        """)
            else:
                st.info("No recent metrics available. Run the ingestion service to see metrics here.")
                
        except Exception as e:
            st.warning(f"Could not fetch metrics: {str(e)}")
            st.info("💡 **To see metrics:** Start Kafka and run the ingestion service")

    with tab3:
        st.header("🗄️ Database Explorer")
        for table in ["students", "catalog", "checkout", "student_similarity"]:
            st.subheader(f"Table: {table}")
            df = get_table_df(table)
            st.dataframe(df)

if __name__ == "__main__":
    logger.info("Streamlit UI starting")
    try:
        main()
    except Exception as e:
        logger.error("Streamlit UI failed", exc_info=True)
        st.error("An error occurred while starting the application.")
        raise 