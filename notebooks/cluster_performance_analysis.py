import os
import sys
import argparse
import pandas as pd
from datetime import datetime, timedelta

# Fix the Python path for notebooks folder
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # Go up one level from notebooks to project root
sys.path.insert(0, project_root)

print(f"Current directory: {current_dir}")
print(f"Project root: {project_root}")

# Now import
from config.settings import DatabricksConfig
from src.monitors.system_tables_client import SystemTablesClient

def setup_safe_display():
    """Setup safe display for Windows console"""
    import logging
    
    class SafeFormatter(logging.Formatter):
        def format(self, record):
            msg = super().format(record)
            return msg.replace('✅', '[OK]').replace('🔄', '[LOADING]').replace('🔍', '[ANALYZING]')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

def print_safe(text):
    """Print with safe encoding"""
    safe_text = text.replace('✅', '[OK]').replace('🔄', '[LOADING]').replace('🔍', '[ANALYZING]')
    safe_text = safe_text.replace('📊', '[STATS]').replace('⚠️', '[WARNING]').replace('💡', '[TIP]')
    safe_text = safe_text.replace('🔴', '[HIGH]').replace('🟡', '[MEDIUM]').replace('🟢', '[LOW]')
    print(safe_text)

class ClusterPerformanceAnalyzer:
    def __init__(self, analysis_days=7, cpu_threshold=80, memory_threshold=85):
        setup_safe_display()
        self.config = DatabricksConfig.from_environment()
        self.client = SystemTablesClient(self.config)
        
        # Configuration
        self.ANALYSIS_DAYS = analysis_days
        self.CPU_ALERT_THRESHOLD = cpu_threshold
        self.MEMORY_ALERT_THRESHOLD = memory_threshold
        self.MIN_DATA_POINTS = 5  # Lowered threshold
        
        print_safe("Cluster Performance Analyzer Initialized")
    
    def get_available_cluster_data(self):
        """Check what cluster data is available"""
        print_safe("Checking available cluster data...")
        
        # Check available schemas
        schemas = self.client.query_system_table("SHOW SCHEMAS IN system")
        print(f"Available system schemas: {schemas['databaseName'].tolist() if not schemas.empty else 'None'}")
        
        # Check compute tables
        if not schemas.empty and 'compute' in schemas['databaseName'].values:
            compute_tables = self.client.query_system_table("SHOW TABLES IN system.compute")
            print(f"Available compute tables: {compute_tables['tableName'].tolist() if not compute_tables.empty else 'None'}")
            
            # Check node_timeline table schema
            if not compute_tables.empty and 'node_timeline' in compute_tables['tableName'].values:
                node_schema = self.client.query_system_table("DESCRIBE system.compute.node_timeline")
                print("\nNode timeline table schema:")
                print(node_schema[['col_name', 'data_type']].head(10).to_string(index=False))
                return True
        
        return False
    
    def get_cluster_utilization_data(self, days=7):
        """Get cluster utilization with error handling"""
        query = f"""
        SELECT 
            cluster_id,
            driver,
            COUNT(*) as data_points,
            ROUND(AVG(COALESCE(cpu_user_percent, 0) + COALESCE(cpu_system_percent, 0)), 2) as avg_cpu_utilization,
            ROUND(MAX(COALESCE(cpu_user_percent, 0) + COALESCE(cpu_system_percent, 0)), 2) as peak_cpu_utilization,
            ROUND(AVG(COALESCE(cpu_wait_percent, 0)), 2) as avg_cpu_wait,
            ROUND(AVG(COALESCE(mem_used_percent, 0)), 2) as avg_memory_utilization,
            ROUND(MAX(COALESCE(mem_used_percent, 0)), 2) as peak_memory_utilization,
            ROUND(AVG(COALESCE(network_received_bytes, 0))/(1024*1024), 2) as avg_network_mb_received_per_minute,
            ROUND(AVG(COALESCE(network_sent_bytes, 0))/(1024*1024), 2) as avg_network_mb_sent_per_minute
        FROM system.compute.node_timeline
        WHERE start_time >= date_sub(current_timestamp(), {days})
        GROUP BY cluster_id, driver
        HAVING COUNT(*) >= {self.MIN_DATA_POINTS}
        ORDER BY avg_cpu_utilization DESC
        """
        return self.client.query_system_table(query)
    
    def get_cluster_basic_info(self):
        """Get basic cluster information with minimal columns"""
        query = """
        SELECT DISTINCT
            cluster_id,
            cluster_name,
            cluster_source,
            change_time
        FROM system.compute.clusters
        WHERE change_time >= date_sub(current_timestamp(), 30)
        ORDER BY change_time DESC
        """
        return self.client.query_system_table(query)
    
    def analyze_performance_issues(self, df):
        """Analyze performance issues safely"""
        issues = []
        
        if df.empty:
            return ["No cluster data available for analysis"]
        
        # High CPU utilization
        high_cpu_clusters = df[df['avg_cpu_utilization'] > self.CPU_ALERT_THRESHOLD]
        for _, cluster in high_cpu_clusters.iterrows():
            severity = 'High' if cluster['avg_cpu_utilization'] > 90 else 'Medium'
            issues.append(f"[{severity.upper()}] High CPU: Cluster {cluster['cluster_id']} - {cluster['avg_cpu_utilization']:.1f}%")
        
        # High memory utilization
        high_memory_clusters = df[df['avg_memory_utilization'] > self.MEMORY_ALERT_THRESHOLD]
        for _, cluster in high_memory_clusters.iterrows():
            severity = 'High' if cluster['avg_memory_utilization'] > 95 else 'Medium'
            issues.append(f"[{severity.upper()}] High Memory: Cluster {cluster['cluster_id']} - {cluster['avg_memory_utilization']:.1f}%")
        
        # High CPU wait
        high_wait_clusters = df[df['avg_cpu_wait'] > 15]
        for _, cluster in high_wait_clusters.iterrows():
            issues.append(f"[MEDIUM] High CPU Wait: Cluster {cluster['cluster_id']} - {cluster['avg_cpu_wait']:.1f}%")
        
        # Underutilized clusters
        underutilized_clusters = df[df['avg_cpu_utilization'] < 20]
        for _, cluster in underutilized_clusters.iterrows():
            issues.append(f"[LOW] Underutilized: Cluster {cluster['cluster_id']} - Only {cluster['avg_cpu_utilization']:.1f}% CPU")
        
        return issues if issues else ["No significant performance issues detected"]
    
    def generate_recommendations(self, df):
        """Generate optimization recommendations"""
        recommendations = []
        
        if df.empty:
            return ["No data available for recommendations"]
        
        # Cost optimization opportunities
        low_usage = df[df['avg_cpu_utilization'] < 30]
        if not low_usage.empty:
            recommendations.append(f"COST OPTIMIZATION: {len(low_usage)} clusters with <30% CPU usage could be downsized")
        
        # Performance optimization
        high_usage = df[df['avg_cpu_utilization'] > 85]
        if not high_usage.empty:
            recommendations.append(f"PERFORMANCE: {len(high_usage)} clusters with >85% CPU usage may need scaling")
        
        # Memory optimization
        high_memory = df[df['avg_memory_utilization'] > 90]
        if not high_memory.empty:
            recommendations.append(f"MEMORY: {len(high_memory)} clusters with >90% memory usage need attention")
        
        # Resource balance
        imbalanced = df[abs(df['avg_cpu_utilization'] - df['avg_memory_utilization']) > 40]
        if not imbalanced.empty:
            recommendations.append(f"BALANCE: {len(imbalanced)} clusters show CPU/Memory imbalance")
        
        return recommendations if recommendations else ["System appears well-optimized"]
    
    def run_analysis(self):
        """Run the complete cluster performance analysis"""
        print_safe("="*80)
        print_safe("CLUSTER PERFORMANCE ANALYSIS")
        print_safe("="*80)
        print(f"Analysis Period: {self.ANALYSIS_DAYS} days")
        print(f"CPU Alert Threshold: {self.CPU_ALERT_THRESHOLD}%")
        print(f"Memory Alert Threshold: {self.MEMORY_ALERT_THRESHOLD}%")
        print_safe("-"*80)
        
        # Check data availability
        if not self.get_available_cluster_data():
            print_safe("No compute data available or insufficient permissions")
            return
        
        # Get cluster performance data
        print_safe("Collecting cluster utilization data...")
        cluster_data = self.get_cluster_utilization_data(self.ANALYSIS_DAYS)
        
        if cluster_data.empty:
            print_safe("No cluster utilization data found for the specified period")
            print_safe("Possible reasons:")
            print("  - No clusters were active in the last 7 days")
            print("  - Insufficient data points (need at least 5 measurements)")
            print("  - System tables may not be fully populated yet")
            return
        
        print_safe(f"Found utilization data for {len(cluster_data)} clusters")
        
        # Get basic cluster info
        print_safe("Getting cluster information...")
        cluster_info = self.get_cluster_basic_info()
        print_safe(f"Retrieved information for {len(cluster_info)} clusters")
        
        # Display summary statistics
        print_safe("\nPERFORMANCE SUMMARY")
        print_safe("-"*50)
        print(f"Clusters analyzed: {len(cluster_data)}")
        print(f"Average CPU utilization: {cluster_data['avg_cpu_utilization'].mean():.2f}%")
        print(f"Average memory utilization: {cluster_data['avg_memory_utilization'].mean():.2f}%")
        print(f"Peak CPU utilization: {cluster_data['peak_cpu_utilization'].max():.2f}%")
        print(f"Peak memory utilization: {cluster_data['peak_memory_utilization'].max():.2f}%")
        
        # Show top resource consumers
        print_safe("\nTOP 5 CPU CONSUMERS")
        print_safe("-"*50)
        top_cpu = cluster_data.nlargest(5, 'avg_cpu_utilization')
        for _, cluster in top_cpu.iterrows():
            driver_type = "Driver" if cluster['driver'] else "Worker"
            print(f"{cluster['cluster_id']} ({driver_type}): {cluster['avg_cpu_utilization']:.1f}% CPU, {cluster['avg_memory_utilization']:.1f}% Memory")
        
        # Show detailed data
        print_safe("\nDETAILED CLUSTER METRICS")
        print_safe("-"*80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(cluster_data.to_string(index=False))
        
        # Analyze issues
        print_safe("\nPERFORMANCE ANALYSIS")
        print_safe("-"*50)
        issues = self.analyze_performance_issues(cluster_data)
        for issue in issues:
            print_safe(f"  {issue}")
        
        # Generate recommendations
        print_safe("\nOPTIMIZATION RECOMMENDATIONS")
        print_safe("-"*50)
        recommendations = self.generate_recommendations(cluster_data)
        for rec in recommendations:
            print_safe(f"  {rec}")
        
        # Save report
        self.save_report(cluster_data, cluster_info, issues, recommendations)
        
        print_safe("\n" + "="*80)
        print_safe("ANALYSIS COMPLETED")
        print_safe("="*80)
    
    def save_report(self, cluster_data, cluster_info, issues, recommendations):
        """Save analysis report as beautified Markdown in project_root/cluster_report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_dir = os.path.join(project_root, "cluster_report")
        os.makedirs(report_dir, exist_ok=True)
        filename = (
            f"cluster_performance_report_{timestamp}.md"
        )
        filepath = os.path.join(report_dir, filename)

        def df_to_markdown(df):
            if df is None or df.empty:
                return "_No data available._"
            try:
                # pandas.to_markdown requires tabulate; fall back if unavailable
                return df.to_markdown(index=False)
            except Exception:
                return "```\n" + df.to_string(index=False) + "\n```"

        def small_table(rows):
            import pandas as _pd
            return df_to_markdown(_pd.DataFrame(rows))

        # Compute time window
        end_time = datetime.now()
        start_time = end_time - timedelta(days=self.ANALYSIS_DAYS)

        # Prepare KPIs and quantiles if data exists
        kpi_rows = []
        quantiles_table = "_No data available._"
        top_cpu_md = "_No data available._"
        top_mem_md = "_No data available._"
        detailed_md = "_No data available._"

        if not cluster_data.empty:
            # Round selected numeric columns for display copies
            display_df = cluster_data.copy()
            numeric_cols = [
                'avg_cpu_utilization', 'peak_cpu_utilization', 'avg_cpu_wait',
                'avg_memory_utilization', 'peak_memory_utilization',
                'avg_network_mb_received_per_minute', 'avg_network_mb_sent_per_minute'
            ]
            for col in numeric_cols:
                if col in display_df.columns:
                    display_df[col] = display_df[col].astype(float).round(2)

            # KPIs
            kpi_rows = [
                {"Metric": "Clusters analyzed", "Value": len(display_df)},
                {"Metric": "Average CPU utilization", "Value": f"{display_df['avg_cpu_utilization'].mean():.2f}%"},
                {"Metric": "Peak CPU utilization", "Value": f"{display_df['peak_cpu_utilization'].max():.2f}%"},
                {"Metric": "Average memory utilization", "Value": f"{display_df['avg_memory_utilization'].mean():.2f}%"},
                {"Metric": "Peak memory utilization", "Value": f"{display_df['peak_memory_utilization'].max():.2f}%"},
            ]

            # Quantiles
            def q(df, col):
                return df[col].quantile([0.5, 0.75, 0.9, 0.95]).round(2).to_dict()
            cpu_q = q(display_df, 'avg_cpu_utilization') if 'avg_cpu_utilization' in display_df.columns else {}
            mem_q = q(display_df, 'avg_memory_utilization') if 'avg_memory_utilization' in display_df.columns else {}
            import pandas as _pd
            if cpu_q and mem_q:
                quant_df = _pd.DataFrame([
                    {"Metric": "CPU utilization", "p50": cpu_q.get(0.5), "p75": cpu_q.get(0.75), "p90": cpu_q.get(0.9), "p95": cpu_q.get(0.95)},
                    {"Metric": "Memory utilization", "p50": mem_q.get(0.5), "p75": mem_q.get(0.75), "p90": mem_q.get(0.9), "p95": mem_q.get(0.95)},
                ])
                quantiles_table = df_to_markdown(quant_df)

            # Top consumers
            top_cpu_cols = ['cluster_id', 'driver', 'avg_cpu_utilization', 'peak_cpu_utilization', 'avg_memory_utilization']
            top_mem_cols = ['cluster_id', 'driver', 'avg_memory_utilization', 'peak_memory_utilization', 'avg_cpu_utilization']
            top_cpu = display_df.sort_values('avg_cpu_utilization', ascending=False).head(10)
            top_mem = display_df.sort_values('avg_memory_utilization', ascending=False).head(10)
            top_cpu_md = df_to_markdown(top_cpu[top_cpu_cols]) if not top_cpu.empty else "_No data available._"
            top_mem_md = df_to_markdown(top_mem[top_mem_cols]) if not top_mem.empty else "_No data available._"

            # Detailed metrics (selected columns)
            detail_cols = [
                'cluster_id', 'driver', 'data_points',
                'avg_cpu_utilization', 'peak_cpu_utilization', 'avg_cpu_wait',
                'avg_memory_utilization', 'peak_memory_utilization',
                'avg_network_mb_received_per_minute', 'avg_network_mb_sent_per_minute'
            ]
            present_cols = [c for c in detail_cols if c in display_df.columns]
            detailed_md = df_to_markdown(display_df[present_cols])

        # Group issues by severity
        issues_by_sev = {"HIGH": [], "MEDIUM": [], "LOW": []}
        for issue in issues or []:
            if issue.startswith("[HIGH]"):
                issues_by_sev["HIGH"].append(issue)
            elif issue.startswith("[MEDIUM]"):
                issues_by_sev["MEDIUM"].append(issue)
            elif issue.startswith("[LOW]"):
                issues_by_sev["LOW"].append(issue)
            else:
                issues_by_sev.setdefault("OTHER", []).append(issue)

        # Group recommendations by theme
        rec_by_theme = {"COST OPTIMIZATION": [], "PERFORMANCE": [], "MEMORY": [], "BALANCE": [], "OTHER": []}
        for rec in recommendations or []:
            if rec.startswith("COST OPTIMIZATION:"):
                rec_by_theme["COST OPTIMIZATION"].append(rec.split(":", 1)[1].strip())
            elif rec.startswith("PERFORMANCE:"):
                rec_by_theme["PERFORMANCE"].append(rec.split(":", 1)[1].strip())
            elif rec.startswith("MEMORY:"):
                rec_by_theme["MEMORY"].append(rec.split(":", 1)[1].strip())
            elif rec.startswith("BALANCE:"):
                rec_by_theme["BALANCE"].append(rec.split(":", 1)[1].strip())
            else:
                rec_by_theme["OTHER"].append(rec)

        # SQL appendix
        utilization_sql = f"""
        SELECT 
            cluster_id,
            driver,
            COUNT(*) as data_points,
            ROUND(AVG(COALESCE(cpu_user_percent, 0) + COALESCE(cpu_system_percent, 0)), 2) as avg_cpu_utilization,
            ROUND(MAX(COALESCE(cpu_user_percent, 0) + COALESCE(cpu_system_percent, 0)), 2) as peak_cpu_utilization,
            ROUND(AVG(COALESCE(cpu_wait_percent, 0)), 2) as avg_cpu_wait,
            ROUND(AVG(COALESCE(mem_used_percent, 0)), 2) as avg_memory_utilization,
            ROUND(MAX(COALESCE(mem_used_percent, 0)), 2) as peak_memory_utilization,
            ROUND(AVG(COALESCE(network_received_bytes, 0))/(1024*1024), 2) as avg_network_mb_received_per_minute,
            ROUND(AVG(COALESCE(network_sent_bytes, 0))/(1024*1024), 2) as avg_network_mb_sent_per_minute
        FROM system.compute.node_timeline
        WHERE start_time >= date_sub(current_timestamp(), {self.ANALYSIS_DAYS})
        GROUP BY cluster_id, driver
        HAVING COUNT(*) >= {self.MIN_DATA_POINTS}
        ORDER BY avg_cpu_utilization DESC
        """.strip()

        clusters_sql = """
        SELECT DISTINCT
            cluster_id,
            cluster_name,
            cluster_source,
            change_time
        FROM system.compute.clusters
        WHERE change_time >= date_sub(current_timestamp(), 30)
        ORDER BY change_time DESC
        """.strip()

        with open(filepath, 'w', encoding='utf-8') as f:
            # Title and metadata
            f.write("# Cluster Performance Analysis Report\n\n")
            f.write("Analysis of Databricks cluster utilization and performance metrics.\n\n")
            f.write("## Run Metadata\n\n")
            f.write(f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"- Time Window: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}\n\n")
            params_rows = [
                {"Parameter": "Analysis days", "Value": self.ANALYSIS_DAYS},
                {"Parameter": "CPU threshold", "Value": f"{self.CPU_ALERT_THRESHOLD}%"},
                {"Parameter": "Memory threshold", "Value": f"{self.MEMORY_ALERT_THRESHOLD}%"},
                {"Parameter": "Min datapoints", "Value": self.MIN_DATA_POINTS},
            ]
            f.write(small_table(params_rows))
            f.write("\n\n")

            # TOC
            f.write("## Table of Contents\n\n")
            f.write("- [Key Metrics](#key-metrics)\n")
            f.write("- [Top Consumers](#top-consumers)\n")
            f.write("- [Performance Issues](#performance-issues)\n")
            f.write("- [Recommendations](#recommendations)\n")
            f.write("- [Detailed Metrics](#detailed-metrics)\n")
            f.write("- [Appendix: SQL Queries](#appendix-sql-queries)\n\n")

            # Key Metrics
            f.write("## Key Metrics\n\n")
            if kpi_rows:
                f.write(small_table(kpi_rows))
                f.write("\n\n")
                f.write("### Quantiles\n\n")
                f.write(quantiles_table + "\n\n")
            else:
                f.write("_No data available._\n\n")

            # Top lists
            f.write("## Top Consumers\n\n")
            f.write("### Top 10 by Average CPU Utilization\n\n")
            f.write(top_cpu_md + "\n\n")
            f.write("### Top 10 by Average Memory Utilization\n\n")
            f.write(top_mem_md + "\n\n")

            # Issues grouped by severity
            f.write("## Performance Issues\n\n")
            total_issues = sum(len(v) for v in issues_by_sev.values())
            if total_issues == 0:
                f.write("_No issues detected._\n\n")
            else:
                for sev in ["HIGH", "MEDIUM", "LOW", "OTHER"]:
                    if sev in issues_by_sev and issues_by_sev[sev]:
                        f.write(f"### {sev} ({len(issues_by_sev[sev])})\n\n")
                        for issue in issues_by_sev[sev]:
                            # Strip the leading [SEV] tag for readability
                            clean = issue
                            if issue.startswith("[") and "]" in issue:
                                clean = issue.split("]", 1)[1].strip()
                            f.write(f"- {clean}\n")
                        f.write("\n")

            # Recommendations grouped by theme
            f.write("## Recommendations\n\n")
            has_rec = any(len(v) > 0 for v in rec_by_theme.values())
            if not has_rec:
                f.write("_No recommendations at this time._\n\n")
            else:
                for theme in ["COST OPTIMIZATION", "PERFORMANCE", "MEMORY", "BALANCE", "OTHER"]:
                    if rec_by_theme.get(theme):
                        f.write(f"### {theme}\n\n")
                        for r in rec_by_theme[theme]:
                            f.write(f"- {r}\n")
                        f.write("\n")

            # Detailed metrics collapsed
            f.write("## Detailed Metrics\n\n")
            f.write("<details><summary>Show detailed metrics table</summary>\n\n")
            f.write(detailed_md + "\n\n")
            f.write("</details>\n\n")

            # Appendix
            f.write("## Appendix: SQL Queries\n\n")
            f.write("<details><summary>Cluster utilization query</summary>\n\n")
            f.write("```sql\n" + utilization_sql + "\n```\n")
            f.write("</details>\n\n")
            f.write("<details><summary>Cluster basic info query</summary>\n\n")
            f.write("```sql\n" + clusters_sql + "\n```\n")
            f.write("</details>\n")

        print_safe(f"Report saved to: {filepath}")
    
    def close(self):
        """Clean up resources"""
        if self.client:
            self.client.close_connections()

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Run cluster performance analysis")
    parser.add_argument("--analysis-days", type=int, default=7, help="Number of days to analyze (default: 7)")
    parser.add_argument("--cpu-threshold", type=int, default=80, help="CPU alert threshold percentage (default: 80)")
    parser.add_argument("--memory-threshold", type=int, default=85, help="Memory alert threshold percentage (default: 85)")
    args = parser.parse_args()

    analyzer = None
    try:
        analyzer = ClusterPerformanceAnalyzer(
            analysis_days=args.analysis_days,
            cpu_threshold=args.cpu_threshold,
            memory_threshold=args.memory_threshold,
        )
        analyzer.run_analysis()
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if analyzer:
            analyzer.close()

if __name__ == "__main__":
    main()
