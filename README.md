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

## Problems Faced & Solutions

### Problem 1: GitHub Push Protection - Exposed AWS Credentials
**Issue:**  
When attempting to push code to GitHub, Push Protection blocked the commit because AWS credentials were accidentally hardcoded in `ingestion_databricks.py` (lines 295-296).

**Error Message:**
```
GitHub Push Protection - Secret Scanning blocked push
AWS Access Key ID detected: AKIAZHSLKEPJQHQ23V4E
```

**Root Cause:**  
During development, AWS credentials were temporarily hardcoded for testing instead of being passed via Databricks widgets or environment variables.

**Solution:**
1. **Immediate Action:** Reset git history to remove the commit with exposed credentials
   ```bash
   git reset --soft HEAD~2
   ```
2. **Remove credentials** from the code file
3. **Re-commit** with clean version
4. **Security Remediation:** Rotated AWS credentials in AWS IAM console
5. **Prevention:** Updated code to use `dbutils.widgets.get()` for credential management in Databricks

**Lesson Learned:**  
Never hardcode credentials in source code. Always use environment variables, secret managers, or configuration files that are added to `.gitignore`.

---

### Problem 2: Missing Output Files After Git Operations
**Issue:**  
After performing git operations (reset and cleanup), January 28th output files were missing from the local workspace, even though they were generated in Databricks.

**Files Missing:**
- 8 runs from January 28, 2026 (16:14 to 20:31)
- Both `channel_stats_*.csv` and `video_details_*.csv` files

**Root Cause:**  
The git reset operation removed local commits that included these files, but they were still present in the remote repository (committed from Databricks).

**Solution:**
1. **Pull from remote** to recover the missing files:
   ```bash
   git pull
   ```
2. **Verified** all 8 sets of files were restored in the `Output/` folder
3. **Resolved merge conflicts** that occurred in the Databricks notebook

**Lesson Learned:**  
Always pull from remote before performing destructive git operations like reset. Keep separate sync workflows for Databricks and local development.

---

### Problem 3: No Log Files Generated in Databricks
**Issue:**  
When running the ingestion notebook in Databricks, no log files were being created in the `/Workspace/.../Logs/` folder, even though logging statements appeared in console output.

**Root Cause:**  
The logging configuration only used `logging.basicConfig()` which outputs to console (StreamHandler) but doesn't create physical log files in Databricks workspace.

**Original Configuration:**
```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
```

**Solution:**
Implemented **dual-handler logging** with both file and console output:

```python
# Generate timestamped log filename
log_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'{DBFS_LOGS_PATH}/log_{log_timestamp}.log'

# Create Logs directory
os.makedirs(DBFS_LOGS_PATH, exist_ok=True)

# Configure logger with both handlers
logger = logging.getLogger('youtube_ingestion')
logger.setLevel(logging.INFO)

# File handler for persistent logs
file_handler = logging.FileHandler(log_filename, mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Console handler for notebook output
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add both handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)
```

**Additional Changes:**
- Changed `DBFS_INPUT_PATH` from `/ingestion/` back to `/input/` to maintain consistency
- Used `os.makedirs()` instead of `dbutils.fs.mkdirs()` for Workspace paths
- Added log file path to pipeline summary output

**Verification:**
Log files now created with naming convention: `log_YYYYMMDD_HHMMSS.log` in the Logs folder.

**Lesson Learned:**  
Databricks workspace paths require explicit file handlers. `logging.basicConfig()` alone is insufficient for persistent logging in Databricks notebooks.

---

### Problem 4: YouTube API Quota Exceeded - Job Failures
**Issue:**  
Databricks scheduled jobs started failing after a few runs due to YouTube Data API v3 quota limits being exceeded. The default quota is 10,000 units per day, and re-fetching all videos on every run quickly exhausted this limit.

**Error Symptoms:**
```
HttpError 403: quotaExceeded
The request cannot be completed because you have exceeded your quota.
```

**Quota Breakdown (Before Optimization):**
- 10 channels × 50 videos each = 500 videos per run
- Channel stats API call: ~3 units × 10 = 30 units
- Video list API calls: ~100 units per channel = 1,000 units
- Video details API calls: ~1 unit per video = 500 units
- **Total per run: ~1,530 units**
- Daily runs (every 6 hours): 4 runs × 1,530 = **6,120 units/day**

This left very little buffer for errors, testing, or additional channels.

**Root Cause:**  
The pipeline was re-fetching **all videos** from each channel on every run, even though most videos were already processed in previous runs. This caused unnecessary API calls for unchanged data.

**Solution - Implemented Incremental Fetching with Caching:**

1. **Video ID Caching:** Created JSON cache files to store previously fetched video IDs
   ```python
   def save_channel_cache(channel_id, video_ids, fetch_time):
       cache_data = {
           'last_fetch_time': fetch_time,
           'video_ids': list(set(video_ids)),
           'updated_at': datetime.datetime.now().isoformat()
       }
       # Save to /cache/channel_{id}_cache.json
   ```

2. **Incremental Mode:** Added `published_after` parameter to fetch only new videos
   ```python
   if incremental and last_fetch_time:
       video_ids = get_video_ids(youtube, channel_id, 
                                 published_after=last_fetch_time)
   ```

3. **Smart Deduplication:** Filter out already-cached video IDs before API calls
   ```python
   new_video_ids = [vid for vid in video_ids if vid not in cached_video_ids]
   video_details = get_video_details(youtube, new_video_ids)
   ```

**Results After Optimization:**
- **First run (full fetch):** ~1,530 units (same as before)
- **Subsequent runs (incremental):** ~50-150 units (90% reduction!)
- Only new videos since last run are fetched
- API quota usage: **200-400 units/day** (well within limits)

**Additional Mitigations:**
- Reduced job schedule from every 6 hours to **daily runs**
- Added `clear_cache()` utility function for manual full refreshes
- Implemented API call savings counter in logs

**Quota Management:**
```python
# Pipeline now shows savings
print(f"💰 API SAVINGS: Skipped ~{saved_calls} API calls by using cache!")
```

**Lesson Learned:**  
Always implement incremental fetching for data pipelines that run frequently. Cache previously processed data and only fetch deltas to minimize API quota usage. Design with quota limits in mind from the start.

---

### Problem 5: Databricks Parameter Management - Secrets vs Widgets
**Issue:**  
Initially attempted to use Databricks Secret Scopes for passing API keys and AWS credentials to notebooks, but faced several difficulties:

**Challenges with Secret Scopes:**
- Complex setup requiring Databricks CLI or Azure Key Vault integration
- Difficult to modify secrets during testing/debugging
- Scope permissions management complexity
- Not ideal for non-sensitive parameters like S3 bucket names or file paths

**Original Approach (Problematic):**
```python
# Attempted secret scope usage
API_KEY = dbutils.secrets.get(scope="youtube-api", key="api-key")
S3_BUCKET = dbutils.secrets.get(scope="aws-secrets", key="bucket-name")
```

**Problems:**
- Required creating secret scopes via CLI
- No visibility into secret values during debugging
- Mixed sensitive (API keys) and non-sensitive (bucket names) parameters
- Difficult to pass dynamic parameters like `CHANNEL_CSV_PATH`

**Solution - Switched to Databricks Job Widgets:**

Implemented **widgets** for parameter management with clearer separation:

```python
# Create widgets for job parameters
dbutils.widgets.text("YOUTUBE_API_KEY", "")
dbutils.widgets.text("S3_BUCKET", "")
dbutils.widgets.text("CHANNEL_CSV_PATH", "")
dbutils.widgets.text("SAVE_FORMAT", "csv")
dbutils.widgets.text("UPLOAD_TO_S3", "true")
dbutils.widgets.text("AWS_ACCESS_KEY_ID", "")
dbutils.widgets.text("AWS_SECRET_ACCESS_KEY", "")

# Retrieve values
YOUTUBE_API_KEY = dbutils.widgets.get("YOUTUBE_API_KEY")
S3_BUCKET = dbutils.widgets.get("S3_BUCKET")
```

**Advantages of Widgets:**
- ✅ Easy to configure in Databricks job UI
- ✅ Can see and modify values without code changes
- ✅ Supports both sensitive and non-sensitive parameters
- ✅ No CLI setup required
- ✅ Values can be passed from job scheduler
- ✅ Better for development and testing

**Best Practice Adopted:**
- **Widgets** for job parameters (both sensitive and configuration)
- **Secret Scopes** reserved for production environments (future enhancement)
- Widget values are masked in job logs for sensitive parameters

**Lesson Learned:**  
Start with widgets for flexibility during development. Migrate to secret scopes for production when security requirements increase. Widgets provide the right balance of usability and functionality for scheduled jobs.

---

### Problem 6: Databricks-Local Development Sync Conflicts
**Issue:**  
Merge conflicts occurred when syncing code between Databricks workspace and local Git repository, particularly in notebook files.

**Solution:**
- Established clear workflow: Develop in Databricks → Commit from Databricks → Pull locally for backup
- Used Git commit messages to track which environment made changes
- Resolved conflicts by accepting Databricks version (more recent)

**Lesson Learned:**  
Maintain a single source of truth (Databricks for active development, Git for version control and backup).

---

### Problem 7: Incremental Fetch Logic Bugs & Data Quality Issues
**Issue:**  
After implementing the initial incremental fetching system, several critical bugs were discovered during production runs that caused incorrect data and potential quota waste:

**Bug 1: Video Duration Parsing Error**
- **Symptom:** All videos in a batch had identical duration values
- **Root Cause:** ISO 8601 duration extraction was outside the per-item loop
  ```python
  # WRONG: duration extracted once, reused for all videos
  duration = isodate.parse_duration(item['contentDetails']['duration'])
  for item in response['items']:
      video_info['duration'] = duration  # Same value for all!
  ```
- **Impact:** Corrupted video duration data, making analytics unreliable
- **Fix:** Moved duration parsing inside the loop for each video item
  ```python
  for item in response['items']:
      video_info = {
          'duration': isodate.parse_duration(
              item['contentDetails']['duration']
          ).total_seconds()
      }
  ```

**Bug 2: Incremental Boundary Missing Videos**
- **Symptom:** Videos uploaded exactly at `last_fetch_time` were missed
- **Root Cause:** Using exact cache timestamp as `publishedAfter` filter caused boundary condition issues
- **Fix:** Implemented **safe_fetch_time** with 10-minute buffer
  ```python
  # Parse stored timestamp and subtract 10 minutes for safety
  last_dt = datetime.datetime.fromisoformat(last_fetch_time.replace('Z', '+00:00'))
  safe_fetch_time = (last_dt - datetime.timedelta(minutes=10)).isoformat() + 'Z'
  ```
- **Benefit:** Ensures no videos are missed due to timing precision issues

**Bug 3: UTC Timestamp Inconsistency**
- **Symptom:** Incremental mode sometimes fetched wrong time ranges
- **Root Cause:** Mixed use of naive and timezone-aware datetime objects
- **Fix:** Standardized all timestamps to UTC-aware format
  ```python
  # Consistent UTC timestamp generation
  fetch_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
  ```
- **Impact:** Ensures cache timestamps match YouTube API's UTC-based `publishedAfter` filter

**Bug 4: Unintended Full Fetches**
- **Symptom:** Incremental mode triggered full fetches unexpectedly
- **Root Cause:** Conditional logic didn't properly distinguish first run vs. subsequent runs
- **Fix:** Added explicit checks for cache availability
  ```python
  if incremental and last_fetch_time:
      # Only apply publishedAfter if we have a safe timestamp
      if safe_fetch_time:
          video_ids = get_video_ids(youtube, channel_id, 
                                    published_after=safe_fetch_time)
  ```

**Enhancement: API Quota Protection Guardrails**

To prevent accidental quota exhaustion, added **MAX_VIDEOS_PER_RUN** limit:

```python
MAX_VIDEOS_PER_RUN = 500  # Cap video details fetches

# Limit video processing per run
if len(new_video_ids) > MAX_VIDEOS_PER_RUN:
    logger.warning(f"Limiting to {MAX_VIDEOS_PER_RUN} videos (found {len(new_video_ids)})")
    new_video_ids = new_video_ids[:MAX_VIDEOS_PER_RUN]
```

**Benefits:**
- ✅ Prevents quota exhaustion from high-volume channels
- ✅ Enables controlled execution cadence (every 3 hours safely)
- ✅ Predictable API usage: ~500 units per run maximum

**Quota Math (After All Fixes):**
- **Every 3 hours:** 8 runs/day × 500 units = 4,000 units/day (within limits)
- **Safety margin:** 6,000 units remaining for errors/testing
- **Controlled growth:** Can add more channels without quota concerns

**Additional Improvements:**
- Enhanced logging for incremental vs. full mode execution
- Better error messages for debugging time-related issues
- Defensive checks to prevent logic bugs
- Improved observability via detailed logs

**Verification Steps:**
1. ✅ Tested first run (full fetch) - works correctly
2. ✅ Tested subsequent run (incremental) - only new videos fetched
3. ✅ Verified duration values are unique per video
4. ✅ Confirmed no missed videos at time boundaries
5. ✅ Validated UTC timestamp consistency
6. ✅ Tested MAX_VIDEOS_PER_RUN cap with high-volume channel

**Lesson Learned:**  
Incremental data pipelines require careful handling of:
- **Time boundaries** (use buffer windows for safety)
- **Timezone awareness** (always use UTC for consistency)
- **Loop variable scope** (extract data inside loops, not outside)
- **Quota safeguards** (add caps to prevent runaway API usage)
- **Edge cases** (first run, cache corruption, boundary conditions)

Test incremental logic thoroughly with multiple runs to catch timing-related bugs.

---

## Key Optimizations Summary

### API Quota Optimization
- **Before:** 1,530 units per run × 4 runs/day = 6,120 units/day
- **After (v1):** 150 units per run × 1 run/day = 150 units/day
- **After (v2 with safeguards):** 500 units per run × 8 runs/day = 4,000 units/day (controlled)
- **Total Savings:** ~35% reduction from original while increasing frequency

### Caching Implementation
- JSON-based cache files stored in `/cache/` directory
- Stores: `last_fetch_time`, `video_ids`, `updated_at`
- Cache persists across job runs
- Manual cache clearing available via `clear_cache()` function
- 10-minute safety buffer to prevent boundary condition misses

### Job Scheduling Evolution
- **Original:** Every 6 hours (4 times daily) - quota exhaustion risk
- **Optimized v1:** Once daily (safe but low freshness)
- **Optimized v2:** Every 3 hours (8 times daily) with MAX_VIDEOS_PER_RUN cap
- **Benefit:** Higher freshness (3-hour lag) with quota protection

### Incremental Logic Improvements
- ✅ Fixed video duration parsing bug (per-item extraction)
- ✅ Implemented safe_fetch_time with 10-minute buffer
- ✅ Standardized UTC timestamp handling
- ✅ Added MAX_VIDEOS_PER_RUN quota guardrail (500 videos/run)
- ✅ Improved conditional logic for first run vs subsequent runs
- ✅ Enhanced logging for better observability

---

## Contributing
This is a personal learning project. Suggestions and feedback are welcome!

## License
MIT License

## Contact
For questions or collaboration opportunities, please open an issue.

---

**Last Updated:** January 29, 2026  
**Version:** 1.2.0 (Incremental Logic Stabilization + Quota Safeguards)
