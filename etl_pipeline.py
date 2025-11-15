"""
ETL Pipeline Module
Handles Extract, Transform, and Load operations
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time


class ETLPipeline:
    """Main ETL Pipeline class"""
    
    def __init__(self):
        self.extracted_data = None
        self.transformed_data = None
        self.load_status = None
        self.logs = []
        
    def log(self, message):
        """Add a log entry"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.logs.append(log_entry)
        return log_entry
    
    def extract(self, source_type="sample"):
        """
        Extract data from various sources
        """
        self.log(f"Starting extraction from {source_type}...")
        time.sleep(0.5)  # Simulate extraction time
        
        if source_type == "sample":
            # Generate sample sales data
            np.random.seed(42)
            dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
            
            data = {
                'date': dates,
                'product_id': np.random.randint(1000, 1100, 100),
                'product_name': [f'Product_{i%10}' for i in range(100)],
                'quantity': np.random.randint(1, 50, 100),
                'price': np.round(np.random.uniform(10, 500, 100), 2),
                'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
                'customer_id': np.random.randint(1, 51, 100)
            }
            
            self.extracted_data = pd.DataFrame(data)
            self.log(f"Extracted {len(self.extracted_data)} records successfully")
            return self.extracted_data
        
        else:
            self.log(f"Unknown source type: {source_type}")
            return None
    
    def transform(self):
        """
        Transform the extracted data
        """
        if self.extracted_data is None:
            self.log("Error: No data to transform. Run extract() first.")
            return None
        
        self.log("Starting transformation...")
        time.sleep(0.5)  # Simulate transformation time
        
        # Create a copy for transformation
        df = self.extracted_data.copy()
        
        # Add calculated fields
        df['revenue'] = df['quantity'] * df['price']
        
        # Add categorization
        df['price_category'] = pd.cut(
            df['price'], 
            bins=[0, 100, 300, 1000],
            labels=['Low', 'Medium', 'High']
        )
        
        # Add time-based features
        df['month'] = df['date'].dt.month
        df['day_of_week'] = df['date'].dt.day_name()
        df['quarter'] = df['date'].dt.quarter
        
        # Clean data - remove any potential duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        removed = initial_count - len(df)
        
        if removed > 0:
            self.log(f"Removed {removed} duplicate records")
        
        # Handle missing values (if any) - only for numeric columns
        numeric_columns = df.select_dtypes(include=['number']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        self.transformed_data = df
        self.log(f"Transformation complete. {len(df)} records transformed")
        return self.transformed_data
    
    def load(self, destination="memory"):
        """
        Load transformed data to destination
        """
        if self.transformed_data is None:
            self.log("Error: No transformed data to load. Run transform() first.")
            return False
        
        self.log(f"Starting load to {destination}...")
        time.sleep(0.5)  # Simulate load time
        
        if destination == "memory":
            # In a real scenario, this would save to a database
            self.load_status = {
                'destination': destination,
                'records_loaded': len(self.transformed_data),
                'timestamp': datetime.now(),
                'status': 'SUCCESS'
            }
            self.log(f"Successfully loaded {len(self.transformed_data)} records to {destination}")
            return True
        
        elif destination == "csv":
            try:
                filename = f"etl_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                self.transformed_data.to_csv(filename, index=False)
                self.load_status = {
                    'destination': destination,
                    'records_loaded': len(self.transformed_data),
                    'timestamp': datetime.now(),
                    'status': 'SUCCESS',
                    'filename': filename
                }
                self.log(f"Successfully loaded {len(self.transformed_data)} records to {filename}")
                return True
            except Exception as e:
                self.log(f"Error loading to CSV: {str(e)}")
                return False
        
        else:
            self.log(f"Unknown destination: {destination}")
            return False
    
    def run_full_pipeline(self, source_type="sample", destination="memory"):
        """
        Run the complete ETL pipeline
        """
        self.log("=" * 50)
        self.log("Starting full ETL pipeline")
        self.log("=" * 50)
        
        # Extract
        extract_result = self.extract(source_type)
        if extract_result is None:
            return False
        
        # Transform
        transform_result = self.transform()
        if transform_result is None:
            return False
        
        # Load
        load_result = self.load(destination)
        
        if load_result:
            self.log("=" * 50)
            self.log("ETL Pipeline completed successfully!")
            self.log("=" * 50)
        else:
            self.log("ETL Pipeline failed during load phase")
        
        return load_result
    
    def get_summary_stats(self):
        """
        Get summary statistics of the transformed data
        """
        if self.transformed_data is None:
            return None
        
        df = self.transformed_data
        
        stats = {
            'total_records': len(df),
            'total_revenue': df['revenue'].sum(),
            'avg_revenue': df['revenue'].mean(),
            'date_range': f"{df['date'].min().date()} to {df['date'].max().date()}",
            'unique_products': df['product_name'].nunique(),
            'unique_customers': df['customer_id'].nunique(),
            'regions': df['region'].nunique()
        }
        
        return stats
