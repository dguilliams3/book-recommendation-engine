import os, requests, streamlit as st
import asyncio
import json
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from common import SettingsInstance as S
from common.structured_logging import get_logger
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import time
from common.redis_utils import get_redis_client

logger = get_logger(__name__)

API_URL = f"http://recommendation_api:{S.api_port}/recommend"
API_TIMEOUT = int(os.getenv("STREAMLIT_API_TIMEOUT_SEC", "30"))
MAX_RETRIES = 3

async def get_latest_metrics():
    """Fetch the latest metrics from Redis instead of Kafka for better reliability"""
    try:
        redis_client = get_redis_client()
        
        # Get recent metrics from Redis sorted set (if available)
        recent_metrics = []
        
        # Try to get ingestion metrics
        try:
            ingestion_key = "metrics:ingestion:recent"
            ingestion_data = await redis_client.lrange(ingestion_key, 0, 4)  # Last 5 metrics
            for data in ingestion_data:
                try:
                    metric = json.loads(data)
                    recent_metrics.append(metric)
                except json.JSONDecodeError:
                    continue
        except Exception:
            pass
            
        # Try to get API metrics  
        try:
            api_key = "metrics:api:recent"
            api_data = await redis_client.lrange(api_key, 0, 4)  # Last 5 metrics
            for data in api_data:
                try:
                    metric = json.loads(data)
                    recent_metrics.append(metric)
                except json.JSONDecodeError:
                    continue
        except Exception:
            pass
            
        # If Redis doesn't have metrics, try Kafka as fallback
        if not recent_metrics:
            return await get_kafka_metrics()
            
        return recent_metrics
        
    except Exception as e:
        logger.warning(f"Redis metrics not available, trying Kafka: {e}")
        return await get_kafka_metrics()

async def get_kafka_metrics():
    """Fallback method to get metrics from Kafka"""
    try:
        # Try Docker Kafka first, then localhost for local development
        kafka_servers = ["kafka:9092", "localhost:9092"]
        
        for server in kafka_servers:
            try:
                consumer = AIOKafkaConsumer(
                    "ingestion_metrics", "api_metrics", "logs",
                    bootstrap_servers=server,
                    group_id="streamlit_dashboard",
                    auto_offset_reset="earliest",  # Changed from latest to get historical data
                    enable_auto_commit=False,
                )
                
                await consumer.start()
                
                # Get recent messages
                metrics = []
                try:
                    # Get all available messages
                    all_messages = await asyncio.wait_for(consumer.getmany(timeout_ms=2000), timeout=5.0)
                    for topic, msgs in all_messages.items():
                        for msg in msgs[-10:]:  # Take last 10 messages per topic
                            try:
                                data = json.loads(msg.value.decode())
                                metrics.append(data)
                            except Exception as e:
                                logger.warning(f"Failed to parse metric message: {e}")
                except asyncio.TimeoutError:
                    # No messages available
                    pass
                finally:
                    await consumer.stop()
                    
                return sorted(metrics, key=lambda x: x.get('timestamp', 0))[-10:]  # Most recent 10
                
            except Exception as e:
                logger.debug(f"Failed to connect to Kafka at {server}: {e}")
                continue
                
        logger.warning("No Kafka servers available")
        return []
        
    except Exception as e:
        logger.warning(f"Kafka not available for metrics: {e}")
        return []

def get_recommendation(student_id: str, interests: str, n: int = 3):
    """Get book recommendation from API (api expects query params)."""
    params = {
        "student_id": student_id,
        "query": interests or "",
        "n": n,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                API_URL,
                params=params,
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.warning(
                "API request timed out", extra={"attempt": attempt, "timeout_sec": API_TIMEOUT}
            )
            if attempt == MAX_RETRIES:
                return {"error": f"API request timed out after {MAX_RETRIES} attempts. Please try again."}
        except requests.exceptions.ConnectionError:
            logger.warning("API service not available")
            return {"error": "API service not running. Please start the recommendation API."}
        except Exception as e:
            logger.error("API request failed", exc_info=True)
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
    tab1, tab2, tab3, tab4 = st.tabs([
        "📖 Book Recommendations",
        "📊 System Metrics",
        "🗄️ Database Explorer",
        "📜 Logs",
    ])
    
    with tab1:
        st.header("📖 Book Recommendations")
        
        with st.form("recommendation_form"):
            student_id = st.text_input("Student ID", value="S001", help="Enter a student ID like S001, S002, etc.")
            interests = st.text_area("Keywords/Interests (comma-separated)", 
                                   value="adventure, animals, space", 
                                   help="Enter keywords describing what the student likes to read about")
            num_recommendations = st.slider("Number of recommendations", 1, 5, 3)
            
            submitted = st.form_submit_button("Get Recommendation")
            
            if submitted:
                with st.spinner("Getting recommendation..."):
                    result = get_recommendation(student_id, interests, num_recommendations)
                    
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
                with st.spinner("Refreshing system status..."):
                    # Force refresh by clearing any cached data
                    st.cache_data.clear()
                    time.sleep(0.5)  # Brief delay to show spinner
                st.success("✅ Metrics refreshed!")
                st.rerun()
        
        # Show recent metrics
        st.subheader("📈 Recent Activity")
        
        try:
            import nest_asyncio
            nest_asyncio.apply()
            metrics = asyncio.get_event_loop().run_until_complete(get_latest_metrics())
            
            if metrics:
                st.write(f"Found {len(metrics)} recent metrics:")
                for metric in metrics[-5:]:  # Show last 5 metrics
                    if metric.get("event") == "ingestion_complete":
                        st.info(f"""
                        **Ingestion Complete** ({metric.get('timestamp', 'N/A')})
                        - Duration: {metric.get('duration', 'N/A')} seconds
                        - Books: {metric.get('books_processed', 'N/A')}
                        - Students: {metric.get('students_processed', 'N/A')}
                        - Checkouts: {metric.get('checkouts_processed', 'N/A')}
                        """)
                    elif metric.get("event") == "recommendation_served":
                        st.success(f"""
                        **Recommendation Served** ({metric.get('timestamp', 'N/A')})
                        - Student: {metric.get('student_id', 'N/A')}
                        - Duration: {metric.get('duration_sec', 'N/A')}s
                        - Tools Used: {metric.get('tool_count', 'N/A')}
                        """)
                    elif metric.get("event") == "api_request":
                        st.success(f"""
                        **API Request** ({metric.get('timestamp', 'N/A')})
                        - Endpoint: {metric.get('endpoint', 'N/A')}
                        - Duration: {metric.get('duration', 'N/A')}ms
                        """)
                    else:
                        # Show any other metrics
                        st.text(f"**{metric.get('event', 'Unknown Event')}**: {json.dumps(metric, indent=2)}")
            else:
                st.info("No recent metrics available.")
                st.info("💡 **To see metrics:** Start Docker services and run:")
                st.code("docker-compose exec ingestion_service python -m ingestion_service.main")
                
        except Exception as e:
            st.warning(f"Could not fetch metrics: {str(e)}")
            st.info("💡 **To enable metrics:** Start Redis, Kafka, and run ingestion service")

    with tab3:
        st.header("🗄️ Database Explorer")
        for table in ["students", "catalog", "checkout", "student_similarity"]:
            st.subheader(f"Table: {table}")
            df = get_table_df(table)
            st.dataframe(df)

    # ---------------------------------------------------------------------
    # Logs tab – lightweight viewer for service_logs.jsonl produced by
    # log_consumer. Shows last N lines with simple filters.
    # ---------------------------------------------------------------------

    with tab4:
        st.header("📜 Service Logs")

        LOG_PATH = Path("logs/service_logs.jsonl")

        if not LOG_PATH.exists():
            st.info("No log file found yet. Ensure log_consumer is running and logs are being published to Kafka.")
        else:
            max_lines = st.number_input("Max lines to load", 1000, 50000, 5000, 1000)

            @st.cache_data(ttl=5, show_spinner=False)
            def _load_logs(lines: int):
                with open(LOG_PATH, "r", encoding="utf-8") as f:
                    data = f.readlines()[-lines:]
                records = [json.loads(l) for l in data]
                return pd.json_normalize(records)

            df = _load_logs(max_lines)

            if df.empty:
                st.info("Log file is empty.")
            else:
                # Filters
                cols = st.columns(3)
                with cols[0]:
                    svcs = sorted(df["service"].dropna().unique())
                    svc_sel = st.multiselect("Service", svcs, default=svcs)
                with cols[1]:
                    lvls = ["DEBUG", "INFO", "WARNING", "ERROR"]
                    lvl_sel = st.multiselect("Level", lvls, default=["INFO", "WARNING", "ERROR"])
                with cols[2]:
                    search = st.text_input("Search text")

                mask = df["service"].isin(svc_sel) & df["level"].isin(lvl_sel)
                if search:
                    mask &= df["event"].str.contains(search, case=False, na=False)

                st.dataframe(
                    df[mask]
                    .sort_values("timestamp", ascending=False)
                    .reset_index(drop=True)
                )

if __name__ == "__main__":
    logger.info("Streamlit UI starting")
    try:
        main()
    except Exception as e:
        logger.error("Streamlit UI failed", exc_info=True)
        st.error("An error occurred while starting the application.")
        raise 