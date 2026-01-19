# AWS S3 Setup Guide

## What's Been Implemented

Your data pipeline now automatically uploads CSV files to AWS S3 after ingestion with this folder structure:

```
s3://your-bucket-name/
└── youtube-raw-data/
    └── 2026/
        └── 01/
            └── 19/
                └── 21/
                    ├── channel_stats_20260119_213221.csv
                    └── video_details_20260119_213221.csv
```

## Setup Steps

### 1. Install boto3 (AWS SDK)
```bash
pip install boto3
```

### 2. Configure AWS Credentials

**Option A: AWS CLI (Recommended)**
```bash
aws configure
```
Enter your:
- AWS Access Key ID
- AWS Secret Access Key
- Default region (e.g., us-east-1)
- Output format (json)

**Option B: Environment Variables**
Add to your `.env` file:
```
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1
```

### 3. Add S3 Bucket Name to .env

Add this line to your `.env` file:
```
S3_BUCKET_NAME=your-bucket-name
```

### 4. Create S3 Bucket (if needed)

Using AWS CLI:
```bash
aws s3 mb s3://your-bucket-name --region us-east-1
```

Or create via AWS Console: https://console.aws.amazon.com/s3/

## How It Works

1. **Ingestion script runs** → Downloads YouTube data
2. **Saves to local CSV** → Creates timestamped files in Output folder
3. **Parses filename** → Extracts YYYYMMDD_HHMMSS from filename
4. **Builds S3 path** → Creates hierarchy: year/month/day/hour/
5. **Uploads to S3** → Stores files in organized structure

## File Structure Example

**Filename**: `video_details_20260119_213221.csv`
- Year: 2026
- Month: 01
- Day: 19
- Hour: 21
- Minute: 32
- Second: 21

**S3 Path**: `youtube-raw-data/2026/01/19/21/video_details_20260119_213221.csv`

## Testing

Run your ingestion script:
```bash
python ingestion.py
```

If S3 is configured correctly, you'll see:
```
✓ Uploaded: Output/channel_stats_20260119_213221.csv -> s3://your-bucket/youtube-raw-data/2026/01/19/21/channel_stats_20260119_213221.csv
✓ Uploaded: Output/video_details_20260119_213221.csv -> s3://your-bucket/youtube-raw-data/2026/01/19/21/video_details_20260119_213221.csv
✓ All files successfully uploaded to S3!
```

## Verify Upload

Check S3 using AWS CLI:
```bash
aws s3 ls s3://your-bucket-name/youtube-raw-data/ --recursive
```

Or visit AWS Console: https://console.aws.amazon.com/s3/

## Next Steps for Databricks

In Databricks, read the data:
```python
# Read from S3
df = spark.read.csv(
    "s3://your-bucket-name/youtube-raw-data/2026/01/19/21/video_details_*.csv",
    header=True,
    inferSchema=True
)

# Or read all data
df = spark.read.csv(
    "s3://your-bucket-name/youtube-raw-data/**/*.csv",
    header=True,
    inferSchema=True
)
```

## Troubleshooting

**Error: "Credentials not available"**
- Run `aws configure` to set up credentials
- Check `.env` file has AWS credentials

**Error: "File not found"**
- Verify CSV files exist in Output folder
- Check file paths are correct

**Error: "Access Denied"**
- Verify S3 bucket exists
- Check IAM permissions for S3 write access
- Ensure bucket name is correct in .env

## Cost Optimization

- S3 Standard: ~$0.023 per GB/month
- Consider S3 Intelligent-Tiering for automatic cost optimization
- Use lifecycle policies to move old data to cheaper storage classes
