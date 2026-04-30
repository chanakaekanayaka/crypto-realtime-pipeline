import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from kafka import KafkaConsumer
import json
from datetime import datetime

# 1. Page Configuration
st.set_page_config(page_title="CryptoStream Intelligence", layout="wide")

# Custom Title and English Descriptions
st.title("Real-Time Bitcoin Analytics Dashboard")
st.markdown("""
    This dashboard visualizes live Bitcoin price data streaming through **Apache Kafka**. 
    Data is fetched from the **Binance API** and processed in real-time.
""")

# 2. Sidebar for System Information (Added Part)
st.sidebar.title("Data Pipeline Info")
st.sidebar.info("""
- **Source:** Binance Public API
- **Streaming:** Apache Kafka
- **Topic:** `crypto-prices`
- **Visualization:** Streamlit & Plotly
""")

# Initialize Session State for Data Storage
if 'data' not in st.session_state:
    st.session_state.data = pd.DataFrame(columns=['Time', 'Price'])

# 3. Layout for Metrics and Charts
col1, col2, col3 = st.columns(3)
price_metric = col1.empty()
high_metric = col2.empty()
low_metric = col3.empty()

chart_placeholder = st.empty()

# 4. Kafka Consumer Initialization
@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        'crypto-prices',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

consumer = get_consumer()

# 5. Live Data Streaming Loop
try:
    for message in consumer:
        price_info = message.value
        
        # Prepare Data
        current_time = datetime.fromtimestamp(price_info['timestamp']).strftime('%H:%M:%S')
        current_price = float(price_info['price'])
        
        new_row = {
            'Time': current_time,
            'Price': current_price
        }
        
        # Update DataFrame (Showing last 30 data points for better visibility)
        st.session_state.data = pd.concat([st.session_state.data, pd.DataFrame([new_row])], ignore_index=True)
        st.session_state.data = st.session_state.data.tail(30) 

        # Calculate Statistics for the small additional part
        session_high = st.session_state.data['Price'].max()
        session_low = st.session_state.data['Price'].min()

        # Update Metrics
        price_metric.metric(label="Current BTC Price (USDT)", value=f"${current_price:,.2f}")
        high_metric.metric(label="Session High", value=f"${session_high:,.2f}")
        low_metric.metric(label="Session Low", value=f"${session_low:,.2f}")

        # Update Live Chart
        with chart_placeholder.container():
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=st.session_state.data['Time'], 
                y=st.session_state.data['Price'],
                mode='lines+markers',
                line=dict(color='#00ff00', width=3),
                marker=dict(size=6, color='white')
            ))
            
            fig.update_layout(
                title="Bitcoin Live Price Movement (Last 30 Updates)",
                xaxis_title="Time Sequence",
                yaxis_title="Price (USDT)",
                template="plotly_dark",
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"System Error: {e}")
    st.info("Please ensure your Kafka Producer and Docker containers are running.")