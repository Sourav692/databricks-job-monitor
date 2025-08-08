import os
import sys
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
    def __init__(self):
        setup_safe_display()
        self.config = DatabricksConfig.from_environment()
        self.client = SystemTablesClient(self.config)
        
        # Configuration
        self.ANALYSIS_DAYS = 7
        self.CPU_ALERT_THRESHOLD = 80
        self.MEMORY_ALERT_THRESHOLD = 85
        self.MIN_DATA_POINTS = 5  # Lowered threshold
        
        print_safe("✅ Cluster Performance Analyzer Initialized")
    
    def get_available_cluster_data(self):
        """Check what cluster data is available"""
        print_safe("🔄 Checking available cluster data...")
        
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
            print_safe("⚠️ No compute data available or insufficient permissions")
            return
        
        # Get cluster performance data
        print_safe("🔄 Collecting cluster utilization data...")
        cluster_data = self.get_cluster_utilization_data(self.ANALYSIS_DAYS)
        
        if cluster_data.empty:
            print_safe("⚠️ No cluster utilization data found for the specified period")
            print_safe("Possible reasons:")
            print("  - No clusters were active in the last 7 days")
            print("  - Insufficient data points (need at least 5 measurements)")
            print("  - System tables may not be fully populated yet")
            return
        
        print_safe(f"✅ Found utilization data for {len(cluster_data)} clusters")
        
        # Get basic cluster info
        print_safe("🔄 Getting cluster information...")
        cluster_info = self.get_cluster_basic_info()
        print_safe(f"✅ Retrieved information for {len(cluster_info)} clusters")
        
        # Display summary statistics
        print_safe("\n📊 PERFORMANCE SUMMARY")
        print_safe("-"*50)
        print(f"Clusters analyzed: {len(cluster_data)}")
        print(f"Average CPU utilization: {cluster_data['avg_cpu_utilization'].mean():.2f}%")
        print(f"Average memory utilization: {cluster_data['avg_memory_utilization'].mean():.2f}%")
        print(f"Peak CPU utilization: {cluster_data['peak_cpu_utilization'].max():.2f}%")
        print(f"Peak memory utilization: {cluster_data['peak_memory_utilization'].max():.2f}%")
        
        # Show top resource consumers
        print_safe("\n📊 TOP 5 CPU CONSUMERS")
        print_safe("-"*50)
        top_cpu = cluster_data.nlargest(5, 'avg_cpu_utilization')
        for _, cluster in top_cpu.iterrows():
            driver_type = "Driver" if cluster['driver'] else "Worker"
            print(f"{cluster['cluster_id']} ({driver_type}): {cluster['avg_cpu_utilization']:.1f}% CPU, {cluster['avg_memory_utilization']:.1f}% Memory")
        
        # Show detailed data
        print_safe("\n📊 DETAILED CLUSTER METRICS")
        print_safe("-"*80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(cluster_data.to_string(index=False))
        
        # Analyze issues
        print_safe("\n🔍 PERFORMANCE ANALYSIS")
        print_safe("-"*50)
        issues = self.analyze_performance_issues(cluster_data)
        for issue in issues:
            print_safe(f"  {issue}")
        
        # Generate recommendations
        print_safe("\n💡 OPTIMIZATION RECOMMENDATIONS")
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
        """Save analysis report to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"cluster_performance_report_{timestamp}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("CLUSTER PERFORMANCE ANALYSIS REPORT\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Analysis Period: {self.ANALYSIS_DAYS} days\n\n")
            
            if not cluster_data.empty:
                f.write("PERFORMANCE SUMMARY\n")
                f.write("-" * 20 + "\n")
                f.write(f"Clusters analyzed: {len(cluster_data)}\n")
                f.write(f"Average CPU utilization: {cluster_data['avg_cpu_utilization'].mean():.2f}%\n")
                f.write(f"Average memory utilization: {cluster_data['avg_memory_utilization'].mean():.2f}%\n\n")
                
                f.write("DETAILED METRICS\n")
                f.write("-" * 20 + "\n")
                f.write(cluster_data.to_string(index=False))
                f.write("\n\n")
            
            f.write("PERFORMANCE ISSUES\n")
            f.write("-" * 20 + "\n")
            for issue in issues:
                f.write(f"{issue}\n")
            
            f.write("\nRECOMMENDATIONS\n")
            f.write("-" * 20 + "\n")
            for rec in recommendations:
                f.write(f"{rec}\n")
        
        print_safe(f"📄 Report saved to: {filename}")
    
    def close(self):
        """Clean up resources"""
        if self.client:
            self.client.close_connections()

def main():
    """Main execution function"""
    analyzer = None
    try:
        analyzer = ClusterPerformanceAnalyzer()
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
