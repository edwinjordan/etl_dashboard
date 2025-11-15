# ETL Dashboard 📊

A modern, interactive ETL (Extract, Transform, Load) dashboard built with Python and Streamlit. This application provides real-time monitoring and visualization of data pipelines.

## Features

- **Interactive Dashboard**: Beautiful web-based interface built with Streamlit
- **Complete ETL Pipeline**: Extract, transform, and load data with comprehensive logging
- **Real-time Analytics**: Interactive charts and visualizations using Plotly
- **Data Exploration**: Filter, preview, and download processed data
- **Performance Metrics**: Track pipeline execution and data statistics
- **Execution Logs**: Detailed logging of all ETL operations

## Screenshots

The dashboard includes:
- **Overview Tab**: Pipeline status and summary metrics
- **Analytics Tab**: Interactive charts (revenue by region, time series, top products)
- **Data Tab**: Filterable data preview with download capability
- **Logs Tab**: Detailed execution logs

## Installation

1. Clone the repository:
```bash
git clone https://github.com/edwinjordan/etl_dashboard.git
cd etl_dashboard
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running the Dashboard

Start the Streamlit dashboard:
```bash
streamlit run dashboard.py
```

The dashboard will open in your browser at `http://localhost:8501`

### Using the Dashboard

1. **Select Data Source**: Choose your data source from the sidebar (currently supports sample data)
2. **Select Destination**: Choose where to load the processed data (memory or CSV)
3. **Run Pipeline**: Click the "Run ETL Pipeline" button
4. **Explore Results**: Navigate through the tabs to view analytics, data, and logs
5. **Download Data**: Export filtered data as CSV from the Data tab

### Using the ETL Pipeline Programmatically

```python
from etl_pipeline import ETLPipeline

# Create pipeline instance
pipeline = ETLPipeline()

# Run complete pipeline
pipeline.run_full_pipeline(source_type="sample", destination="memory")

# Or run stages individually
pipeline.extract("sample")
pipeline.transform()
pipeline.load("csv")

# Get summary statistics
stats = pipeline.get_summary_stats()
print(stats)
```

## Project Structure

```
etl_dashboard/
├── dashboard.py          # Streamlit dashboard application
├── etl_pipeline.py       # ETL pipeline implementation
├── requirements.txt      # Python dependencies
├── .gitignore           # Git ignore rules
└── README.md            # This file
```

## ETL Pipeline Details

### Extract
- Generates sample sales data with realistic patterns
- Includes: dates, products, quantities, prices, regions, customers
- Extensible to support multiple data sources

### Transform
- Calculates derived fields (revenue)
- Adds categorization (price categories)
- Extracts time-based features (month, quarter, day of week)
- Removes duplicates and handles missing values
- Comprehensive data cleaning

### Load
- Supports multiple destinations (memory, CSV)
- Includes error handling and validation
- Tracks load status and metrics
- Extensible for database integration

## Dependencies

- **streamlit**: Web dashboard framework
- **pandas**: Data manipulation and analysis
- **plotly**: Interactive visualizations
- **numpy**: Numerical computing

## Development

### Adding New Data Sources

Extend the `extract()` method in `etl_pipeline.py`:

```python
def extract(self, source_type="sample"):
    if source_type == "database":
        # Add database extraction logic
        pass
    elif source_type == "api":
        # Add API extraction logic
        pass
```

### Adding New Transformations

Extend the `transform()` method in `etl_pipeline.py`:

```python
def transform(self):
    # Add custom transformations
    df['custom_field'] = df['field1'] + df['field2']
    # ...
```

### Adding New Load Destinations

Extend the `load()` method in `etl_pipeline.py`:

```python
def load(self, destination="memory"):
    if destination == "database":
        # Add database load logic
        pass
```

## Future Enhancements

- [ ] Database connectivity (PostgreSQL, MySQL, MongoDB)
- [ ] API data source integration
- [ ] Scheduled pipeline execution
- [ ] Email notifications for pipeline status
- [ ] Data quality checks and validation rules
- [ ] Multi-user authentication
- [ ] Pipeline configuration via YAML/JSON
- [ ] Error recovery and retry mechanisms

## License

MIT License - feel free to use this project for learning and development.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

Built with ❤️ using Python and Streamlit