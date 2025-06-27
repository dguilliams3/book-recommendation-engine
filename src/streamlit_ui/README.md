# Streamlit UI Dashboard

## Overview

The **Streamlit UI Dashboard** serves as the primary interface for educators and system administrators, providing a comprehensive real-time view of the book recommendation system. This service demonstrates advanced patterns in reactive UI design, real-time data visualization, and distributed system monitoring.

## Architecture & User Experience

### **Multi-Modal Interface Design**
- **Teacher Portal**: Intuitive book recommendation interface for classroom use
- **System Operations**: Real-time monitoring dashboard for technical teams
- **Data Explorer**: Interactive database browsing with live query capabilities
- **Observability Console**: Structured log analysis and metrics visualization

### **Real-Time Data Integration**
```python
# Multi-source data aggregation with fault tolerance
async def get_latest_metrics():
    try:
        # Primary: Redis for low-latency access
        redis_client = get_redis_client()
        recent_metrics = await fetch_redis_metrics(redis_client)
        
        # Fallback: Kafka for real-time streams  
        if not recent_metrics:
            return await get_kafka_metrics()
    except Exception:
        return await get_kafka_metrics()  # Graceful degradation
```

## Core Interface Components

### **1. Book Recommendation Portal**
```python
with st.form("recommendation_form"):
    student_id = st.text_input("Student ID", value="S001")
    interests = st.text_area("Keywords/Interests", 
                           value="adventure, animals, space")
    num_recommendations = st.slider("Number of recommendations", 1, 5, 3)
```

**Features**:
- **Input Validation**: Real-time validation of student IDs and query parameters
- **Smart Defaults**: Pre-populated fields based on usage patterns
- **Progressive Enhancement**: Graceful fallbacks for API connectivity issues
- **Response Formatting**: Structured display of recommendation results with justifications

### **2. System Metrics Dashboard**
```python
# Real-time system health monitoring
col1, col2 = st.columns(2)
with col1:
    st.metric("API Response Time", f"{avg_latency:.2f}s", 
              delta=f"{latency_delta:.2f}s")
with col2:
    st.metric("Success Rate", f"{success_rate:.1f}%", 
              delta=f"{rate_delta:.1f}%")
```

**Monitoring Capabilities**:
- **Service Health**: Real-time status of all microservices
- **Performance Metrics**: Latency distributions and throughput measurements  
- **Error Tracking**: Categorized error rates with trend analysis
- **Resource Utilization**: Database and cache performance indicators

### **3. Interactive Data Explorer**
```python
@st.cache_data(ttl=30, show_spinner=False)
def get_table_data(table_name, limit=100):
    engine = create_engine(str(S.db_url).replace("+asyncpg", ""))
    with engine.connect() as conn:
        return pd.read_sql(f"SELECT * FROM {table_name} LIMIT {limit}", conn)
```

**Database Integration**:
- **Live Queries**: Direct database access with caching for performance
- **Table Browser**: Interactive exploration of all system tables
- **Data Visualization**: Dynamic charts and graphs for data analysis
- **Export Capabilities**: CSV download functionality for further analysis

### **4. Observability & Logging**
```python
# Structured log aggregation with search capabilities
@st.cache_data(ttl=5, show_spinner=False)
def load_recent_logs(lines: int = 50):
    log_file = Path("logs/service_logs.jsonl")
    if not log_file.exists():
        return []
    
    return [json.loads(line) for line in log_file.read_text().splitlines()[-lines:]]
```

**Log Analysis Features**:
- **Real-Time Streaming**: Live log updates with auto-refresh
- **Filtering & Search**: Advanced filtering by service, level, and content
- **Error Correlation**: Linking related errors across service boundaries
- **Performance Profiling**: Request tracing and bottleneck identification

## Advanced UI Patterns

### **Reactive Data Loading**
```python
# Efficient data loading with fallback strategies
try:
    import nest_asyncio
    nest_asyncio.apply()  # Enable async in Streamlit environment
    
    metrics = asyncio.run(get_latest_metrics())
    if metrics:
        st.success(f"✅ Real-time data ({len(metrics)} events)")
    else:
        st.warning("⚠️ Using cached data")
except Exception as e:
    st.error(f"❌ Data unavailable: {str(e)}")
```

### **Progressive Data Visualization**
- **Layered Information**: Summary cards with drill-down capabilities
- **Responsive Design**: Adaptive layouts for different screen sizes
- **Performance Optimization**: Intelligent caching and data pagination
- **Accessibility**: Screen reader support and keyboard navigation

## System Integration Points

### **API Communication**
```python
# Robust API integration with comprehensive error handling
def get_recommendation(student_id: str, interests: str, n: int = 3):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(API_URL, params=params, timeout=API_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            if attempt == MAX_RETRIES:
                return {"error": "API timeout - please try again"}
        except requests.exceptions.ConnectionError:
            return {"error": "API service unavailable"}
```

### **Real-Time Data Sources**

#### **Redis Integration**
- **Metrics Cache**: Sub-second access to recent performance data
- **Session State**: User preferences and interface state persistence
- **Rate Limiting**: Request throttling and quota management

#### **Kafka Streaming**
```python
# Event stream consumption for real-time updates
consumer = AIOKafkaConsumer(
    "ingestion_metrics", "api_metrics", "logs",
    bootstrap_servers=kafka_servers,
    group_id="streamlit_dashboard",
    auto_offset_reset="earliest"
)
```

#### **Database Direct Access**
- **Live Queries**: Real-time data exploration with connection pooling
- **Performance Optimization**: Query caching and result pagination
- **Transaction Safety**: Read-only access with isolation level controls

## User Experience Design

### **Information Architecture**
- **Tab-Based Navigation**: Logical separation of functional areas
- **Progressive Disclosure**: Essential information first, details on demand
- **Contextual Help**: In-line documentation and usage hints
- **Mobile Responsiveness**: Optimized layouts for tablet and mobile access

### **Performance Optimization**
```python
# Multi-layer caching strategy
@st.cache_data(ttl=60, show_spinner=False)        # App-level cache
def expensive_computation():
    # Database queries and aggregations
    pass

@st.cache_resource                                  # Resource-level cache
def get_database_connection():
    # Connection pooling and reuse
    pass
```

### **Error Handling & Resilience**
- **Graceful Degradation**: Partial functionality when services are unavailable
- **User Feedback**: Clear error messages with actionable guidance
- **Retry Mechanisms**: Automatic retries with user-controlled manual retries
- **Offline Capabilities**: Cached data access when connectivity is limited

## Deployment & Configuration

### **Environment Configuration**
```bash
STREAMLIT_API_TIMEOUT_SEC=30              # API request timeout
DB_URL=postgresql://...                   # Database connection
REDIS_URL=redis://redis:6379              # Cache connection
KAFKA_BROKERS=kafka:9092                  # Event streaming
STREAMLIT_SERVER_PORT=8501                # Web server port
```

### **Container Deployment**
```dockerfile
# Optimized Streamlit container
FROM python:3.12-slim
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

### **Security Considerations**
- **Input Sanitization**: Prevention of injection attacks through user inputs
- **Session Management**: Secure handling of user sessions and preferences  
- **Access Control**: Role-based access to sensitive operational data
- **Data Privacy**: No persistent storage of student data in UI layer

## Monitoring & Analytics

### **User Behavior Tracking**
- **Feature Usage**: Analytics on most-used dashboard components
- **Performance Metrics**: Page load times and interaction responsiveness
- **Error Rates**: User-facing error frequency and categorization
- **Session Analytics**: User engagement patterns and workflow analysis

### **System Health Indicators**
```python
# Comprehensive system status dashboard
system_status = {
    "api_health": check_api_availability(),
    "database_health": check_database_connectivity(),
    "kafka_health": check_kafka_connectivity(),
    "redis_health": check_redis_availability()
}
```

### **Real-Time Alerts**
- **Service Degradation**: Visual indicators for service availability issues
- **Performance Thresholds**: Alerts when response times exceed baselines
- **Data Freshness**: Warnings when real-time data becomes stale
- **Error Spikes**: Immediate notification of unusual error patterns

## Development Workflow

### **Local Development**
```bash
# Install dependencies
pip install -r requirements.txt

# Run with hot-reload
streamlit run app.py --server.runOnSave=true

# Development with debug logging
STREAMLIT_LOGGER_LEVEL=debug streamlit run app.py
```

### **Testing Strategy**
- **UI Component Tests**: Isolated testing of individual dashboard components
- **Integration Tests**: End-to-end user workflow validation
- **Performance Tests**: Load testing under realistic user scenarios
- **Accessibility Tests**: Screen reader and keyboard navigation validation

---

**Strategic Value**: This interface serves as the primary touchpoint for educators and system operators, directly impacting user adoption and operational efficiency. Its intuitive design and comprehensive monitoring capabilities enable effective system management and educational outcomes. 