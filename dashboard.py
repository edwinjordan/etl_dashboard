"""
ETL Dashboard - Streamlit Application
A dashboard to monitor and execute ETL pipelines
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from etl_pipeline import ETLPipeline

# Page configuration
st.set_page_config(
    page_title="ETL Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'etl_pipeline' not in st.session_state:
    st.session_state.etl_pipeline = ETLPipeline()
if 'pipeline_run' not in st.session_state:
    st.session_state.pipeline_run = False

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown('<div class="main-header">📊 ETL Dashboard</div>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("⚙️ Pipeline Controls")
    
    st.subheader("Data Source")
    source_type = st.selectbox(
        "Select Source",
        ["sample"],
        help="Choose the data source for extraction"
    )
    
    st.subheader("Load Destination")
    destination = st.selectbox(
        "Select Destination",
        ["memory", "csv"],
        help="Choose where to load the transformed data"
    )
    
    st.markdown("---")
    
    # Run Pipeline Button
    if st.button("▶️ Run ETL Pipeline", type="primary", use_container_width=True):
        with st.spinner("Running ETL Pipeline..."):
            st.session_state.etl_pipeline = ETLPipeline()
            success = st.session_state.etl_pipeline.run_full_pipeline(source_type, destination)
            st.session_state.pipeline_run = True
            
            if success:
                st.success("✅ Pipeline completed successfully!")
            else:
                st.error("❌ Pipeline failed!")
    
    # Reset Button
    if st.button("🔄 Reset Pipeline", use_container_width=True):
        st.session_state.etl_pipeline = ETLPipeline()
        st.session_state.pipeline_run = False
        st.rerun()
    
    st.markdown("---")
    st.caption("Built with Streamlit 📊")

# Main content area
if not st.session_state.pipeline_run:
    # Welcome screen
    st.info("👈 Click 'Run ETL Pipeline' in the sidebar to start")
    
    st.subheader("About This Dashboard")
    st.write("""
    This ETL (Extract, Transform, Load) Dashboard provides:
    
    - **Extract**: Pull data from various sources (currently: sample data)
    - **Transform**: Clean, enrich, and process the data
    - **Load**: Store the processed data to a destination
    
    ### Features:
    - Real-time pipeline monitoring
    - Interactive data visualization
    - Performance metrics and statistics
    - Detailed execution logs
    
    ### How to Use:
    1. Select your data source and destination in the sidebar
    2. Click 'Run ETL Pipeline' to execute
    3. View the results and analytics below
    """)

else:
    # Show pipeline results
    etl = st.session_state.etl_pipeline
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Overview", "📊 Analytics", "📋 Data", "📝 Logs"])
    
    with tab1:
        st.header("Pipeline Overview")
        
        # Summary Statistics
        if etl.transformed_data is not None:
            stats = etl.get_summary_stats()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="Total Records",
                    value=f"{stats['total_records']:,}"
                )
            
            with col2:
                st.metric(
                    label="Total Revenue",
                    value=f"${stats['total_revenue']:,.2f}"
                )
            
            with col3:
                st.metric(
                    label="Unique Products",
                    value=stats['unique_products']
                )
            
            with col4:
                st.metric(
                    label="Unique Customers",
                    value=stats['unique_customers']
                )
            
            st.markdown("---")
            
            # Pipeline Status
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Pipeline Stages")
                stages = [
                    {"stage": "Extract", "status": "✅ Complete", "records": len(etl.extracted_data)},
                    {"stage": "Transform", "status": "✅ Complete", "records": len(etl.transformed_data)},
                    {"stage": "Load", "status": "✅ Complete" if etl.load_status else "❌ Failed", 
                     "records": etl.load_status['records_loaded'] if etl.load_status else 0}
                ]
                st.dataframe(pd.DataFrame(stages), use_container_width=True, hide_index=True)
            
            with col2:
                st.subheader("Date Range")
                st.info(f"📅 {stats['date_range']}")
                
                if etl.load_status:
                    st.subheader("Load Status")
                    st.success(f"✅ {etl.load_status['status']}")
                    st.caption(f"Destination: {etl.load_status['destination']}")
                    st.caption(f"Timestamp: {etl.load_status['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    with tab2:
        st.header("Data Analytics")
        
        if etl.transformed_data is not None:
            df = etl.transformed_data
            
            # Revenue by Region
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Revenue by Region")
                region_revenue = df.groupby('region')['revenue'].sum().reset_index()
                fig = px.pie(
                    region_revenue,
                    values='revenue',
                    names='region',
                    title='Revenue Distribution by Region',
                    hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Revenue by Price Category")
                price_cat_revenue = df.groupby('price_category')['revenue'].sum().reset_index()
                fig = px.bar(
                    price_cat_revenue,
                    x='price_category',
                    y='revenue',
                    title='Revenue by Price Category',
                    color='revenue',
                    color_continuous_scale='blues'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Time series
            st.subheader("Revenue Over Time")
            daily_revenue = df.groupby('date')['revenue'].sum().reset_index()
            fig = px.line(
                daily_revenue,
                x='date',
                y='revenue',
                title='Daily Revenue Trend',
                markers=True
            )
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Revenue ($)",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Top products
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Top 10 Products by Revenue")
                top_products = df.groupby('product_name')['revenue'].sum().sort_values(ascending=False).head(10)
                fig = px.bar(
                    x=top_products.values,
                    y=top_products.index,
                    orientation='h',
                    title='Top 10 Products',
                    labels={'x': 'Revenue ($)', 'y': 'Product'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Revenue by Quarter")
                quarter_revenue = df.groupby('quarter')['revenue'].sum().reset_index()
                quarter_revenue['quarter'] = 'Q' + quarter_revenue['quarter'].astype(str)
                fig = px.bar(
                    quarter_revenue,
                    x='quarter',
                    y='revenue',
                    title='Quarterly Revenue',
                    color='revenue',
                    color_continuous_scale='viridis'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.header("Data Preview")
        
        if etl.transformed_data is not None:
            st.subheader("Transformed Data")
            
            # Filter options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                regions = ['All'] + sorted(etl.transformed_data['region'].unique().tolist())
                selected_region = st.selectbox("Filter by Region", regions)
            
            with col2:
                products = ['All'] + sorted(etl.transformed_data['product_name'].unique().tolist())
                selected_product = st.selectbox("Filter by Product", products)
            
            with col3:
                show_rows = st.slider("Rows to display", 10, 100, 50)
            
            # Apply filters
            filtered_df = etl.transformed_data.copy()
            
            if selected_region != 'All':
                filtered_df = filtered_df[filtered_df['region'] == selected_region]
            
            if selected_product != 'All':
                filtered_df = filtered_df[filtered_df['product_name'] == selected_product]
            
            st.dataframe(
                filtered_df.head(show_rows),
                use_container_width=True,
                hide_index=True
            )
            
            # Download button
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Filtered Data as CSV",
                data=csv,
                file_name=f"etl_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
            # Data Statistics
            st.subheader("Column Statistics")
            st.dataframe(
                filtered_df.describe(),
                use_container_width=True
            )
    
    with tab4:
        st.header("Pipeline Execution Logs")
        
        if etl.logs:
            # Display logs in a code block
            st.code('\n'.join(etl.logs), language='log')
        else:
            st.info("No logs available. Run the pipeline to see execution logs.")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>ETL Dashboard v1.0 | Built with Python & Streamlit</p>
    </div>
    """,
    unsafe_allow_html=True
)
