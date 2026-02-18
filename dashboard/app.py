"""
Customer 360 Analytics Dashboard
==================================
Interactive Streamlit dashboard showcasing the pipeline's
analytics capabilities with live data visualizations.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Customer 360 | Analytics Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .stApp {
        font-family: 'Inter', sans-serif;
    }

    .main-header {
        background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
        padding: 2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        color: white;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    }

    .main-header h1 {
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0;
        background: linear-gradient(90deg, #fff, #a78bfa);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    .main-header p {
        color: #c4b5fd;
        font-size: 1rem;
        margin-top: 0.3rem;
    }

    .metric-card {
        background: linear-gradient(135deg, #1e1b4b 0%, #312e81 100%);
        padding: 1.5rem;
        border-radius: 14px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 20px rgba(99, 102, 241, 0.15);
        border: 1px solid rgba(139, 92, 246, 0.2);
        transition: transform 0.2s ease;
    }

    .metric-card:hover {
        transform: translateY(-2px);
    }

    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(90deg, #818cf8, #c084fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    .metric-label {
        font-size: 0.85rem;
        color: #a5b4fc;
        margin-top: 0.3rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .section-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #e0e7ff;
        margin: 1.5rem 0 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid rgba(139, 92, 246, 0.3);
    }

    .pipeline-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        margin: 0.2rem;
    }

    .badge-green {
        background: rgba(34, 197, 94, 0.15);
        color: #4ade80;
        border: 1px solid rgba(34, 197, 94, 0.3);
    }

    .stSidebar > div:first-child {
        background: linear-gradient(180deg, #0f0c29 0%, #1e1b4b 100%);
    }

    div[data-testid="stSidebar"] .stMarkdown {
        color: #e0e7ff;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
    }
</style>
""", unsafe_allow_html=True)


# ── Data Loading ─────────────────────────────────────────────
@st.cache_data
def generate_sample_data():
    """Generate realistic sample data in memory for cloud deployment."""
    import random
    import numpy as np

    random.seed(42)
    np.random.seed(42)

    # ── Customers ────────────────────────────────────────────
    n_customers = 2000
    first_names = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
                   "William","Elizabeth","David","Barbara","Richard","Susan","Joseph","Jessica",
                   "Thomas","Sarah","Charles","Karen","Christopher","Lisa","Daniel","Nancy",
                   "Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley",
                   "Steven","Dorothy","Paul","Kimberly","Andrew","Emily","Joshua","Donna"]
    last_names = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
                  "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
                  "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson",
                  "White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson"]
    states = ["CA","TX","NY","FL","IL","PA","OH","GA","NC","MI","NJ","VA","WA","AZ","MA",
              "TN","IN","MO","MD","WI","CO","MN","SC","AL","LA","KY","OR","OK","CT","UT"]
    segments = ["Premium", "Standard", "Basic"]
    genders = ["Male", "Female", "Non-Binary"]

    cust_records = []
    for i in range(1, n_customers + 1):
        reg_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))
        cust_records.append({
            "customer_id": f"CUST-{i:06d}",
            "first_name": random.choice(first_names),
            "last_name": random.choice(last_names),
            "email": f"user{i}@example.com",
            "gender": random.choice(genders),
            "date_of_birth": datetime(1960, 1, 1) + timedelta(days=random.randint(0, 18000)),
            "registration_date": reg_date,
            "address_state": random.choice(states),
            "customer_segment": random.choices(segments, weights=[20, 50, 30])[0],
            "lifetime_value": round(random.gauss(1500, 800), 2),
        })
    customers = pd.DataFrame(cust_records)
    customers['lifetime_value'] = customers['lifetime_value'].clip(lower=10)
    customers['registration_date'] = pd.to_datetime(customers['registration_date'])
    customers['date_of_birth'] = pd.to_datetime(customers['date_of_birth'])

    # ── Products ─────────────────────────────────────────────
    categories = ["Electronics","Clothing","Home & Garden","Sports","Books",
                  "Beauty","Toys","Food & Beverage","Automotive","Health"]
    prod_records = []
    for i in range(1, 201):
        cat = random.choice(categories)
        prod_records.append({
            "product_id": f"PROD-{i:04d}",
            "product_name": f"{cat} Item {i}",
            "category": cat,
            "price": round(random.uniform(5, 500), 2),
        })
    products = pd.DataFrame(prod_records)

    # ── Transactions ─────────────────────────────────────────
    n_transactions = 20000
    channels = ["web", "mobile", "in_store", "partner"]
    payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]
    tx_records = []
    for i in range(1, n_transactions + 1):
        prod = products.sample(1).iloc[0]
        qty = random.randint(1, 5)
        unit_price = prod['price']
        discount = round(random.uniform(0, unit_price * 0.3), 2) if random.random() < 0.35 else 0
        total = round(qty * unit_price - discount, 2)
        tx_records.append({
            "transaction_id": f"TX-{i:07d}",
            "customer_id": f"CUST-{random.randint(1, n_customers):06d}",
            "product_id": prod['product_id'],
            "transaction_date": datetime(2024, 1, 1) + timedelta(hours=random.randint(0, 8760)),
            "quantity": qty,
            "unit_price": unit_price,
            "discount_amount": discount,
            "total_amount": max(total, 1.0),
            "channel": random.choices(channels, weights=[40, 30, 20, 10])[0],
            "payment_method": random.choice(payment_methods),
        })
    transactions = pd.DataFrame(tx_records)
    transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'])

    # ── Clickstream ──────────────────────────────────────────
    n_events = 10000
    event_types = ["page_view", "search", "product_view", "add_to_cart", "purchase", "remove_from_cart"]
    event_weights = [35, 20, 20, 12, 8, 5]
    click_channels = ["web", "mobile", "tablet"]
    cs_records = []
    for i in range(1, n_events + 1):
        cs_records.append({
            "event_id": f"EVT-{i:08d}",
            "event_type": random.choices(event_types, weights=event_weights)[0],
            "event_timestamp": datetime(2025, 1, 1) + timedelta(seconds=random.randint(0, 2592000)),
            "customer_id": f"CUST-{random.randint(1, n_customers):06d}",
            "session_id": f"SESS-{random.randint(1, 3000):06d}",
            "channel": random.choices(click_channels, weights=[50, 35, 15])[0],
        })
    clickstream = pd.DataFrame(cs_records)
    clickstream['event_timestamp'] = pd.to_datetime(clickstream['event_timestamp'])

    return customers, products, transactions, clickstream


@st.cache_data
def load_data():
    """Load sample data from files or generate on-the-fly for cloud deployment."""
    base = Path(__file__).parent.parent / "sample_data" / "output"

    if (base / "customers.csv").exists():
        customers = pd.read_csv(base / "customers.csv")
        products = pd.read_csv(base / "products.csv")
        transactions = pd.read_csv(base / "transactions.csv")

        customers['registration_date'] = pd.to_datetime(customers['registration_date'])
        customers['date_of_birth'] = pd.to_datetime(customers['date_of_birth'])
        transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'])

        clickstream_path = base / "clickstream.json"
        clickstream_records = []
        if clickstream_path.exists():
            with open(clickstream_path) as f:
                for line in f:
                    if line.strip():
                        clickstream_records.append(json.loads(line))
        clickstream = pd.DataFrame(clickstream_records)
        if 'event_timestamp' in clickstream.columns:
            clickstream['event_timestamp'] = pd.to_datetime(clickstream['event_timestamp'])

        return customers, products, transactions, clickstream
    else:
        return generate_sample_data()


customers, products, transactions, clickstream = load_data()
data_loaded = True


# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🎛️ Navigation")
    page = st.radio(
        "Select View",
        ["📊 Executive Overview", "👥 Customer Analytics", "🛒 Transaction Insights",
         "📱 Clickstream Analysis", "🏗️ Pipeline Architecture"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("### 🔧 Pipeline Status")
    st.markdown("""
    <span class="pipeline-badge badge-green">● Kinesis Stream</span>
    <span class="pipeline-badge badge-green">● Lambda Consumer</span>
    <span class="pipeline-badge badge-green">● Glue ETL</span>
    <span class="pipeline-badge badge-green">● Redshift</span>
    <span class="pipeline-badge badge-green">● Data Quality</span>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 📁 Data Layers")
    if data_loaded:
        st.markdown(f"**Raw:** {len(transactions):,} transactions")
        st.markdown(f"**Clean:** {len(customers):,} customers")
        st.markdown(f"**Curated:** {len(products):,} products")
        if len(clickstream) > 0:
            st.markdown(f"**Stream:** {len(clickstream):,} events")





# ── Helper Functions ─────────────────────────────────────────
def format_currency(val):
    if val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    elif val >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:.0f}"


CHART_THEME = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {"color": "#e0e7ff", "family": "Inter"},
    "colorway": ["#818cf8", "#c084fc", "#f472b6", "#fb923c", "#34d399", "#38bdf8", "#fbbf24"],
}


def style_chart(fig, height=400):
    fig.update_layout(
        **CHART_THEME,
        height=height,
        margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(gridcolor="rgba(139,92,246,0.1)", zeroline=False)
    fig.update_yaxes(gridcolor="rgba(139,92,246,0.1)", zeroline=False)
    return fig


# ── PAGES ────────────────────────────────────────────────────

if page == "📊 Executive Overview":
    st.markdown("""
    <div class="main-header">
        <h1>🚀 Customer 360 Platform</h1>
        <p>Real-Time Data Engineering Pipeline on AWS — Executive Dashboard</p>
    </div>
    """, unsafe_allow_html=True)

    # KPI Cards
    total_revenue = transactions['total_amount'].sum()
    avg_order = transactions['total_amount'].mean()
    total_customers = len(customers)
    total_transactions = len(transactions)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{format_currency(total_revenue)}</div>
            <div class="metric-label">Total Revenue</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total_customers:,}</div>
            <div class="metric-label">Total Customers</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total_transactions:,}</div>
            <div class="metric-label">Transactions</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">${avg_order:.2f}</div>
            <div class="metric-label">Avg Order Value</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("")

    # Revenue Trend
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown('<div class="section-header">📈 Monthly Revenue Trend</div>', unsafe_allow_html=True)
        monthly = transactions.groupby(transactions['transaction_date'].dt.to_period('M')).agg(
            revenue=('total_amount', 'sum'),
            orders=('transaction_id', 'count')
        ).reset_index()
        monthly['transaction_date'] = monthly['transaction_date'].astype(str)

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(
            x=monthly['transaction_date'], y=monthly['revenue'],
            name="Revenue", marker_color="#818cf8", opacity=0.8
        ))
        fig.add_trace(go.Scatter(
            x=monthly['transaction_date'], y=monthly['orders'],
            name="Orders", line=dict(color="#f472b6", width=3),
            mode="lines+markers"
        ), secondary_y=True)
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="Order Count", secondary_y=True)
        st.plotly_chart(style_chart(fig, 380), use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">🥧 Revenue by Channel</div>', unsafe_allow_html=True)
        channel_rev = transactions.groupby('channel')['total_amount'].sum().reset_index()
        fig = px.pie(channel_rev, values='total_amount', names='channel',
                     hole=0.55, color_discrete_sequence=CHART_THEME["colorway"])
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(style_chart(fig, 380), use_container_width=True)

    # Customer Segments & Top Products
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">👥 Customer Segments</div>', unsafe_allow_html=True)
        seg = customers['customer_segment'].value_counts().reset_index()
        seg.columns = ['segment', 'count']
        fig = px.bar(seg, x='segment', y='count', color='segment',
                     color_discrete_sequence=["#818cf8", "#c084fc", "#f472b6"])
        fig.update_layout(showlegend=False)
        st.plotly_chart(style_chart(fig, 350), use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">🏆 Top Products by Revenue</div>', unsafe_allow_html=True)
        tx_products = transactions.merge(products[['product_id', 'product_name', 'category']], on='product_id', how='left')
        top_products = tx_products.groupby('category')['total_amount'].sum().nlargest(8).reset_index()
        fig = px.bar(top_products, x='total_amount', y='category', orientation='h',
                     color='total_amount', color_continuous_scale='Purples')
        fig.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(style_chart(fig, 350), use_container_width=True)


elif page == "👥 Customer Analytics":
    st.markdown("""
    <div class="main-header">
        <h1>👥 Customer Analytics</h1>
        <p>Segmentation, Lifetime Value Distribution, and Registration Trends</p>
    </div>
    """, unsafe_allow_html=True)

    # LTV Distribution
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">💰 Lifetime Value Distribution</div>', unsafe_allow_html=True)
        fig = px.histogram(customers, x='lifetime_value', nbins=50,
                           color='customer_segment',
                           color_discrete_sequence=["#818cf8", "#c084fc", "#f472b6"],
                           barmode='overlay', opacity=0.7)
        fig.update_xaxes(title_text="Lifetime Value ($)")
        st.plotly_chart(style_chart(fig, 400), use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">📊 LTV by Segment</div>', unsafe_allow_html=True)
        seg_ltv = customers.groupby('customer_segment')['lifetime_value'].agg(['mean', 'median', 'count']).reset_index()
        seg_ltv.columns = ['Segment', 'Mean LTV', 'Median LTV', 'Count']
        fig = px.bar(seg_ltv, x='Segment', y='Mean LTV', color='Segment',
                     text='Count', color_discrete_sequence=["#818cf8", "#c084fc", "#f472b6"])
        fig.update_traces(texttemplate='n=%{text:,}', textposition='outside')
        st.plotly_chart(style_chart(fig, 400), use_container_width=True)

    # Registration Trend
    st.markdown('<div class="section-header">📅 Customer Registration Trend</div>', unsafe_allow_html=True)
    reg_monthly = customers.groupby(customers['registration_date'].dt.to_period('M')).size().reset_index(name='new_customers')
    reg_monthly['registration_date'] = reg_monthly['registration_date'].astype(str)
    fig = px.area(reg_monthly, x='registration_date', y='new_customers',
                  color_discrete_sequence=["#818cf8"])
    fig.update_traces(fill='tozeroy', fillcolor='rgba(129,140,248,0.2)')
    st.plotly_chart(style_chart(fig, 350), use_container_width=True)

    # Demographics
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">🌍 Geographic Distribution</div>', unsafe_allow_html=True)
        state_counts = customers['address_state'].value_counts().nlargest(15).reset_index()
        state_counts.columns = ['state', 'customers']
        fig = px.bar(state_counts, x='state', y='customers', color='customers',
                     color_continuous_scale='Purples')
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(style_chart(fig, 350), use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">👤 Gender Distribution</div>', unsafe_allow_html=True)
        gender = customers['gender'].value_counts().reset_index()
        gender.columns = ['gender', 'count']
        fig = px.pie(gender, values='count', names='gender', hole=0.5,
                     color_discrete_sequence=CHART_THEME['colorway'])
        st.plotly_chart(style_chart(fig, 350), use_container_width=True)


elif page == "🛒 Transaction Insights":
    st.markdown("""
    <div class="main-header">
        <h1>🛒 Transaction Insights</h1>
        <p>Revenue Analytics, Payment Methods, and Purchase Patterns</p>
    </div>
    """, unsafe_allow_html=True)

    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        avg_qty = transactions['quantity'].mean()
        st.markdown(f'<div class="metric-card"><div class="metric-value">{avg_qty:.1f}</div><div class="metric-label">Avg Items/Order</div></div>', unsafe_allow_html=True)
    with c2:
        discount_rate = (transactions['discount_amount'] > 0).mean() * 100
        st.markdown(f'<div class="metric-card"><div class="metric-value">{discount_rate:.0f}%</div><div class="metric-label">Discount Rate</div></div>', unsafe_allow_html=True)
    with c3:
        avg_discount = transactions.loc[transactions['discount_amount'] > 0, 'discount_amount'].mean()
        st.markdown(f'<div class="metric-card"><div class="metric-value">${avg_discount:.2f}</div><div class="metric-label">Avg Discount</div></div>', unsafe_allow_html=True)
    with c4:
        unique_customers = transactions['customer_id'].nunique()
        st.markdown(f'<div class="metric-card"><div class="metric-value">{unique_customers:,}</div><div class="metric-label">Active Buyers</div></div>', unsafe_allow_html=True)

    st.markdown("")

    # Charts
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">💳 Payment Methods</div>', unsafe_allow_html=True)
        pay = transactions['payment_method'].value_counts().reset_index()
        pay.columns = ['method', 'count']
        fig = px.bar(pay, x='method', y='count', color='method',
                     color_discrete_sequence=CHART_THEME['colorway'])
        fig.update_layout(showlegend=False)
        st.plotly_chart(style_chart(fig, 380), use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">📊 Order Value Distribution</div>', unsafe_allow_html=True)
        fig = px.histogram(transactions, x='total_amount', nbins=60,
                           color_discrete_sequence=["#818cf8"])
        fig.update_xaxes(title_text="Order Value ($)")
        st.plotly_chart(style_chart(fig, 380), use_container_width=True)

    # Daily transaction volume
    st.markdown('<div class="section-header">📆 Daily Transaction Volume</div>', unsafe_allow_html=True)
    daily = transactions.groupby(transactions['transaction_date'].dt.date).agg(
        volume=('transaction_id', 'count'),
        revenue=('total_amount', 'sum')
    ).reset_index()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(
        x=daily['transaction_date'], y=daily['volume'],
        name="Volume", fill='tozeroy', fillcolor='rgba(129,140,248,0.2)',
        line=dict(color="#818cf8")
    ))
    fig.add_trace(go.Scatter(
        x=daily['transaction_date'], y=daily['revenue'],
        name="Revenue", line=dict(color="#f472b6", width=2)
    ), secondary_y=True)
    st.plotly_chart(style_chart(fig, 380), use_container_width=True)

    # Revenue by channel over time
    st.markdown('<div class="section-header">📡 Channel Performance Over Time</div>', unsafe_allow_html=True)
    channel_monthly = transactions.groupby([transactions['transaction_date'].dt.to_period('M'), 'channel'])['total_amount'].sum().reset_index()
    channel_monthly['transaction_date'] = channel_monthly['transaction_date'].astype(str)
    fig = px.area(channel_monthly, x='transaction_date', y='total_amount', color='channel',
                  color_discrete_sequence=CHART_THEME['colorway'])
    st.plotly_chart(style_chart(fig, 380), use_container_width=True)


elif page == "📱 Clickstream Analysis":
    st.markdown("""
    <div class="main-header">
        <h1>📱 Clickstream Analysis</h1>
        <p>Real-Time Event Stream Analytics from Kinesis Pipeline</p>
    </div>
    """, unsafe_allow_html=True)

    if len(clickstream) == 0:
        st.warning("No clickstream data found. Run: `python sample_data/generators/generate_clickstream.py`")
        st.stop()

    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{len(clickstream):,}</div><div class="metric-label">Total Events</div></div>', unsafe_allow_html=True)
    with c2:
        sessions = clickstream['session_id'].nunique()
        st.markdown(f'<div class="metric-card"><div class="metric-value">{sessions:,}</div><div class="metric-label">Sessions</div></div>', unsafe_allow_html=True)
    with c3:
        unique_users = clickstream['customer_id'].nunique()
        st.markdown(f'<div class="metric-card"><div class="metric-value">{unique_users:,}</div><div class="metric-label">Unique Users</div></div>', unsafe_allow_html=True)
    with c4:
        events_per_session = len(clickstream) / max(sessions, 1)
        st.markdown(f'<div class="metric-card"><div class="metric-value">{events_per_session:.1f}</div><div class="metric-label">Events/Session</div></div>', unsafe_allow_html=True)

    st.markdown("")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">🎯 Event Types</div>', unsafe_allow_html=True)
        event_counts = clickstream['event_type'].value_counts().reset_index()
        event_counts.columns = ['event_type', 'count']
        fig = px.bar(event_counts, x='event_type', y='count', color='event_type',
                     color_discrete_sequence=CHART_THEME['colorway'])
        fig.update_layout(showlegend=False)
        st.plotly_chart(style_chart(fig, 380), use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">📡 Channel Mix</div>', unsafe_allow_html=True)
        ch = clickstream['channel'].value_counts().reset_index()
        ch.columns = ['channel', 'count']
        fig = px.pie(ch, values='count', names='channel', hole=0.55,
                     color_discrete_sequence=["#818cf8", "#c084fc", "#f472b6"])
        st.plotly_chart(style_chart(fig, 380), use_container_width=True)

    # Hourly event volume
    st.markdown('<div class="section-header">⏰ Hourly Event Volume</div>', unsafe_allow_html=True)
    if 'event_timestamp' in clickstream.columns:
        clickstream['hour'] = clickstream['event_timestamp'].dt.hour
        hourly = clickstream.groupby('hour').size().reset_index(name='events')
        fig = px.bar(hourly, x='hour', y='events', color='events',
                     color_continuous_scale='Purples')
        fig.update_layout(coloraxis_showscale=False)
        fig.update_xaxes(title_text="Hour of Day", dtick=1)
        st.plotly_chart(style_chart(fig, 350), use_container_width=True)

    # Conversion Funnel
    st.markdown('<div class="section-header">🔄 Conversion Funnel</div>', unsafe_allow_html=True)
    funnel_data = clickstream['event_type'].value_counts()
    funnel_stages = ['page_view', 'search', 'add_to_cart', 'purchase']
    funnel_values = [funnel_data.get(s, 0) for s in funnel_stages]
    fig = go.Figure(go.Funnel(
        y=funnel_stages, x=funnel_values,
        textinfo="value+percent initial",
        marker=dict(color=["#818cf8", "#a78bfa", "#c084fc", "#e879f9"])
    ))
    st.plotly_chart(style_chart(fig, 400), use_container_width=True)


elif page == "🏗️ Pipeline Architecture":
    st.markdown("""
    <div class="main-header">
        <h1>🏗️ Pipeline Architecture</h1>
        <p>End-to-End AWS Data Engineering Pipeline — System Overview</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    ### Architecture Overview

    ```
    ┌─────────────────┐     ┌───────────────────┐     ┌──────────────────┐
    │  Kinesis Stream  │────▶│  Lambda Consumer   │────▶│   S3 Raw Layer   │
    │  (Clickstream)   │     │  (JSON → S3)       │     │   (JSON/CSV)     │
    └─────────────────┘     └───────────────────┘     └────────┬─────────┘
                                                               │
    ┌─────────────────┐                                        │
    │  Batch CSV       │────────────────────────────────────────┘
    │  Uploader        │                              ┌────────▼─────────┐
    └─────────────────┘                               │  Glue ETL Job    │
                                                      │  (Raw → Clean)   │
                                                      └────────┬─────────┘
                                                               │
                                                      ┌────────▼─────────┐
                                                      │  S3 Clean Layer  │
                                                      │  (Parquet)       │
                                                      └────────┬─────────┘
                                                               │
    ┌─────────────────┐                               ┌────────▼─────────┐
    │  Data Quality    │◀─────────────────────────────│  Glue ETL Job    │
    │  Gate            │─── FAIL ──▶ Quarantine       │  (Clean→Curated) │
    └────────┬────────┘                               └────────┬─────────┘
             │ PASS                                            │
    ┌────────▼─────────┐                              ┌────────▼─────────┐
    │  S3 Curated      │                              │  Redshift Star   │
    │  (Star Schema)   │─────────────────────────────▶│  Schema          │
    └──────────────────┘    Glue ETL (COPY+MERGE)     └────────┬─────────┘
                                                               │
    ┌─────────────────┐                               ┌────────▼─────────┐
    │  Step Functions  │ ─── orchestrates all ───────▶ │  BI Dashboards   │
    │  + EventBridge   │                               │  (This App!)     │
    └─────────────────┘                               └──────────────────┘
    ```
    """)

    st.markdown("---")

    # Tech stack
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        ### 🔄 Ingestion
        - **Kinesis Data Streams** — Real-time
        - **Lambda** — Stream processing
        - **S3 Upload** — Batch files

        ### 🗃️ Data Lake
        - **S3** — 3-layer architecture
        - **Glue Catalog** — Schema registry
        - **Parquet** — Columnar storage
        """)
    with col2:
        st.markdown("""
        ### ⚙️ Processing
        - **Glue PySpark** — ETL engine
        - **Star Schema** — Dim modeling
        - **Redshift** — OLAP warehouse

        ### 🛡️ Quality & Security
        - **JSON Schema** — Validation
        - **IAM** — Least privilege
        - **KMS** — Encryption at rest
        """)
    with col3:
        st.markdown("""
        ### 📋 Orchestration
        - **Step Functions** — Workflow
        - **EventBridge** — Scheduling
        - **Quality Gate** — Auto-halt

        ### 📊 Monitoring
        - **CloudWatch** — Alarms + Dash
        - **SNS** — Email alerts
        - **Terraform** — IaC (8 modules)
        """)

    st.markdown("---")
    st.markdown("### 📂 Project Stats")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Source Files", "48+")
    with c2:
        st.metric("Terraform Modules", "8")
    with c3:
        st.metric("Unit Tests", "18")
    with c4:
        st.metric("Analytics Queries", "10")
