import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from typing import Dict, List
import streamlit as st

class JobMonitoringDashboard:
    """Interactive dashboard for job monitoring metrics"""
    
    def __init__(self, job_monitor: JobMonitor):
        self.monitor = job_monitor
    
    def create_runtime_trend_chart(self, metrics: pd.DataFrame) -> go.Figure:
        """Create job runtime trend visualization"""
        fig = px.box(
            metrics, 
            x='job_name', 
            y='avg_duration_seconds',
            title='Job Runtime Distribution',
            labels={
                'avg_duration_seconds': 'Runtime (seconds)',
                'job_name': 'Job Name'
            }
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            height=500,
            showlegend=False
        )
        
        return fig
    
    def create_success_rate_chart(self, failure_metrics: pd.DataFrame) -> go.Figure:
        """Create success rate visualization"""
        fig = px.bar(
            failure_metrics.head(20),  # Top 20 jobs
            x='job_name',
            y='success_rate_percent',
            color='success_rate_percent',
            title='Job Success Rates',
            labels={
                'success_rate_percent': 'Success Rate (%)',
                'job_name': 'Job Name'
            },
            color_continuous_scale='RdYlGn'
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            height=500
        )
        
        return fig
    
    def create_cluster_utilization_chart(self, cluster_metrics: pd.DataFrame) -> go.Figure:
        """Create cluster utilization visualization"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'CPU Utilization', 'Memory Utilization',
                'Network Received (MB/min)', 'Network Sent (MB/min)'
            )
        )
        
        # CPU Utilization
        fig.add_trace(
            go.Scatter(
                x=cluster_metrics['cluster_id'],
                y=cluster_metrics['avg_cpu_utilization'],
                mode='markers+lines',
                name='Avg CPU %',
                marker=dict(color='blue')
            ),
            row=1, col=1
        )
        
        # Memory Utilization
        fig.add_trace(
            go.Scatter(
                x=cluster_metrics['cluster_id'],
                y=cluster_metrics['avg_memory_utilization'],
                mode='markers+lines',
                name='Avg Memory %',
                marker=dict(color='green')
            ),
            row=1, col=2
        )
        
        # Network Metrics
        fig.add_trace(
            go.Scatter(
                x=cluster_metrics['cluster_id'],
                y=cluster_metrics['avg_network_mb_received_per_minute'],
                mode='markers+lines',
                name='Network In (MB/min)',
                marker=dict(color='red')
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=cluster_metrics['cluster_id'],
                y=cluster_metrics['avg_network_mb_sent_per_minute'],
                mode='markers+lines',
                name='Network Out (MB/min)',
                marker=dict(color='orange')
            ),
            row=2, col=2
        )
        
        fig.update_layout(
            height=600,
            title_text="Cluster Resource Utilization Metrics"
        )
        
        return fig
    
    def render_streamlit_dashboard(self):
        """Render interactive Streamlit dashboard"""
        st.title("Databricks Job Monitoring Dashboard")
        st.sidebar.title("Configuration")
        
        # Configuration inputs
        days = st.sidebar.slider("Days to analyze", min_value=1, max_value=30, value=7)
        refresh_data = st.sidebar.button("Refresh Data")
        
        if refresh_data or 'metrics_data' not in st.session_state:
            with st.spinner("Fetching metrics data..."):
                st.session_state.metrics_data = self.monitor.get_comprehensive_job_metrics(days)
        
        metrics = st.session_state.metrics_data
        
        if not metrics:
            st.error("No metrics data available. Please check your configuration.")
            return
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
            with col1:
                total_jobs = len(metrics['runtime_metrics'])
                st.metric("Total Jobs", total_jobs)
            
            with col2:
                avg_runtime = metrics['runtime_metrics']['avg_duration_seconds'].mean() / 60
                st.metric("Avg Runtime (min)", f"{avg_runtime:.2f}")
        
        if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
            with col3:
                avg_success_rate = metrics['failure_analysis']['success_rate_percent'].mean()
                st.metric("Avg Success Rate (%)", f"{avg_success_rate:.1f}")
            
            with col4:
                total_failures = metrics['failure_analysis']['failed_runs'].sum()
                st.metric("Total Failures", total_failures)
        
        # Charts
        if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
            st.subheader("Job Runtime Analysis")
            runtime_chart = self.create_runtime_trend_chart(metrics['runtime_metrics'])
            st.plotly_chart(runtime_chart, use_container_width=True)
        
        if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
            st.subheader("Job Success Rates")
            success_chart = self.create_success_rate_chart(metrics['failure_analysis'])
            st.plotly_chart(success_chart, use_container_width=True)
        
        if 'cluster_utilization' in metrics and not metrics['cluster_utilization'].empty:
            st.subheader("Cluster Resource Utilization")
            cluster_chart = self.create_cluster_utilization_chart(metrics['cluster_utilization'])
            st.plotly_chart(cluster_chart, use_container_width=True)
        
        # Detailed tables
        st.subheader("Detailed Metrics")
        tab1, tab2, tab3 = st.tabs(["Runtime Metrics", "Failure Analysis", "Cluster Utilization"])
        
        with tab1:
            if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
                st.dataframe(metrics['runtime_metrics'])
        
        with tab2:
            if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
                st.dataframe(metrics['failure_analysis'])
        
        with tab3:
            if 'cluster_utilization' in metrics and not metrics['cluster_utilization'].empty:
                st.dataframe(metrics['cluster_utilization'])
