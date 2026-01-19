# YouTube Analytics Data Pipeline

## Overview
An end-to-end data engineering project that extracts YouTube channel and video analytics data using the YouTube Data API v3, stores it in AWS S3 with a hierarchical data lake structure, and prepares it for transformation and analytics in Databricks.

This pipeline implements data engineering best practices including automated ingestion, comprehensive logging, error handling, and organized data storage for efficient querying and analysis.

## Project Status
✅ **Phase 1 Complete:** Data Ingestion & S3 Upload  
🔄 **Phase 2 In Progress:** Databricks Integration & Transformation  
⏳ **Phase 3 Planned:** Analytics & Visualization

## Tech Stack
- **Language:** Python 3.14
- **APIs:** YouTube Data API v3
- **Cloud Storage:** AWS S3
- **Data Processing:** PySpark (Databricks) - *Upcoming*
- **Data Format:** CSV (Bronze Layer), Delta Lake (Silver/Gold) - *Upcoming*
- **Libraries:** 
  - google-api-python-client (YouTube API)
  - pandas (Data manipulation)
  - boto3 (AWS S3)
  - isodate (Duration parsing)
  - python-dotenv (Environment management)

## Architecture

### Current Implementation (Bronze Layer)
```
YouTube API → Python Ingestion Script → Local CSV → AWS S3 (Hierarchical Structure)
                                            ↓
                                    Logs/ & Output/ folders
```

### Future Architecture
```
YouTube API → Ingestion → S3 Bronze → Databricks → S3 Silver → S3 Gold → BI Tools
                            ↓           (PySpark)      ↓          ↓
                         Raw CSV      Cleaned Data  Analytics  Dashboards
```

## Data Model

### Channel-Level Data
- Channel ID
- Channel Name
- Subscriber Count
- Total Views
- Video Count

### Video-Level Data
- Video ID
- Title
- Published Date
- View Count
- Like Count
- Comment Count
- Duration (seconds)
- Tags (comma-separated)
- Category ID
- Language

## Project Structure
```
youtube-analytics-data-pipeline/
├── ingestion.py              # Main data extraction script
├── rawdata_migration.py      # S3 upload with hierarchical structure
├── top10channelid.csv        # Channel IDs for batch processing
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (not in git)
├── README.md                # This file
├── S3_SETUP.md              # AWS S3 configuration guide
├── Logs/                    # Execution logs (timestamped)
│   └── log_YYYYMMDD_HHMMSS.log
└── Output/                  # Generated CSV files
    ├── channel_stats_YYYYMMDD_HHMMSS.csv
    └── video_details_YYYYMMDD_HHMMSS.csv
```

## S3 Data Lake Structure
```
s3://your-bucket-name/
└── youtube-raw-data/
    └── YYYY/              # Year partition
        └── MM/            # Month partition
            └── DD/        # Day partition
                └── HH/    # Hour partition
                    ├── channel_stats_YYYYMMDD_HHMMSS.csv
                    └── video_details_YYYYMMDD_HHMMSS.csv
```

## Setup Instructions

### 1. Clone Repository
```bash
git clone <repository-url>
cd youtube-analytics-data-pipeline
```

### 2. Create Virtual Environment
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows PowerShell
# or
source .venv/bin/activate   # Linux/Mac
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Create a `.env` file in the project root:
```env
# YouTube API
YOUTUBE_API_KEY=your_youtube_api_key_here

# AWS S3 (Optional - for S3 upload)
S3_BUCKET_NAME=your-s3-bucket-name
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
```

### 5. Get YouTube API Key
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project
3. Enable YouTube Data API v3
4. Create credentials (API Key)
5. Copy the API key to your `.env` file

### 6. (Optional) Setup AWS S3
See [S3_SETUP.md](S3_SETUP.md) for detailed AWS configuration instructions.

## How to Run

### Execute the Pipeline
```bash
python ingestion.py
```

### What Happens
1. ✅ Reads channel IDs from `top10channelid.csv`
2. ✅ Fetches channel statistics for each channel
3. ✅ Retrieves video IDs and details for each channel
4. ✅ Consolidates all data into 2 CSV files (one for channels, one for videos)
5. ✅ Saves CSV files locally in `Output/` folder
6. ✅ Uploads to S3 with hierarchical folder structure (if configured)
7. ✅ Logs all operations to `Logs/` folder

### Expected Output
```
Processing channel ID: UCX6OQ3DkcsbYNE6H8uQQuVA
Fetching channel statistics for UCX6OQ3DkcsbYNE6H8uQQuVA...
Channel: MrBeast
Subscribers: 460000000
...
Creating final CSV files...
All data saved to:
  - Output/channel_stats_20260119_213221.csv
  - Output/video_details_20260119_213221.csv

Uploading files to S3 bucket: my-bucket...
✓ Uploaded: Output/channel_stats_20260119_213221.csv -> s3://my-bucket/youtube-raw-data/2026/01/19/21/...
✓ All files successfully uploaded to S3!
```

## Features

### ✅ Implemented
- Batch processing of multiple YouTube channels
- Comprehensive data extraction (channel + video metrics)
- Automated timestamp-based file naming
- Hierarchical S3 data lake structure
- Detailed logging system
- Error handling for API and S3 operations
- Environment-based configuration
- CSV consolidation (single file per run)

### 🔄 In Progress
- Databricks integration
- PySpark transformations
- Delta Lake implementation

### ⏳ Planned
- Incremental data ingestion
- Schema validation
- Data quality checks
- Automated scheduling
- Monitoring and alerts
- Dashboard integration

## Configuration

### Channel Selection
Edit `top10channelid.csv` to process different channels:
```csv
channel_name,channel_id
MrBeast,UCX6OQ3DkcsbYNE6H8uQQuVA
PewDiePie,UC-lHJZR3Gqxm24_Vd_AJ5Yw
...
```

### S3 Upload
S3 upload is optional and automatically skipped if not configured. The pipeline will still work without AWS credentials.

## Troubleshooting

### "ModuleNotFoundError: No module named 'googleapiclient'"
```bash
pip install -r requirements.txt
```

### "AWS credentials not found"
Add AWS credentials to `.env` file or run `aws configure`

### "API quota exceeded"
YouTube API has daily quotas. Wait 24 hours or request quota increase from Google.

## API Quotas
- YouTube Data API v3: 10,000 units/day (default)
- Each channel stats request: ~3 units
- Each video details request: ~3 units
- 10 channels with 50 videos each ≈ 180 units

## Next Steps

### Phase 2: Databricks Integration
- [ ] Create Databricks workspace
- [ ] Mount S3 bucket to Databricks
- [ ] Implement PySpark transformations
- [ ] Create Delta Lake tables

### Phase 3: Analytics Layer
- [ ] Design analytics schema
- [ ] Create aggregation tables
- [ ] Build dashboards
- [ ] Schedule automated runs

## Daily Development Log

### January 19, 2026
**🎉 Milestone: Bronze Layer Complete**

**Achievements:**
- ✅ Built complete data ingestion pipeline using YouTube Data API v3
- ✅ Implemented batch processing for 10 YouTube channels
- ✅ Created hierarchical S3 data lake structure (year/month/day/hour partitions)
- ✅ Added comprehensive logging system with timestamped log files
- ✅ Implemented consolidated CSV output (single file per data type)
- ✅ Added AWS S3 integration with automatic upload
- ✅ Enriched video data with channel ID and name columns
- ✅ Created detailed documentation and setup guides
- ✅ Implemented error handling for API calls and S3 uploads

**Technical Details:**
- Extracted data: Channel stats (5 fields) + Video details (10 fields)
- Processed: 10 channels with 50 videos each (API limit)
- Output format: 2 CSV files per run (channel_stats + video_details)
- S3 path: `youtube-raw-data/YYYY/MM/DD/HH/filename.csv`
- Logging: Both console output and file-based logging

**Code Quality:**
- Added comprehensive inline comments explaining every function
- Documented data engineering best practices
- Included detailed docstrings for all functions
- Created requirements.txt for dependency management

**Challenges Solved:**
- ✅ Multiple CSV files issue → Consolidated to single file per type
- ✅ boto3 installation in virtual environment
- ✅ S3 hierarchical path creation from filename timestamps
- ✅ Duplicate data on multiple runs → Timestamp-based unique files

**Files Created:**
- `ingestion.py` (377 lines)
- `rawdata_migration.py` (326 lines)
- `requirements.txt`
- `top10channelid.csv`
- `S3_SETUP.md`
- Updated `README.md`

**Next Session Goals:**
- Set up Databricks workspace
- Mount S3 bucket to Databricks
- Begin PySpark transformation development
- Implement data quality checks

**Learning Notes:**
- Understood regex pattern matching for timestamp parsing
- Learned AWS S3 hierarchical data lake design patterns
- Practiced environment variable management with dotenv
- Explored pandas DataFrame consolidation techniques

---

## Contributing
This is a personal learning project. Suggestions and feedback are welcome!

## License
MIT License

## Contact
For questions or collaboration opportunities, please open an issue.

---

**Last Updated:** January 19, 2026  
**Version:** 1.0.0 (Bronze Layer Complete)
