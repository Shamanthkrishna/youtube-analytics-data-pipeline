

'''
YouTube Analytics Data Ingestion Script - Databricks Version

This script extracts comprehensive data from YouTube channels using the YouTube Data API v3.
Optimized for Databricks environment with DBFS integration.

Data Extracted:
Channel-level:
    - Channel ID
    - Channel name
    - Subscribers count
    - Total views
    - Video count

Video-level:
    - Video ID
    - Title
    - Published date
    - View count
    - Like count
    - Comment count
    - Duration

Optional (bonus):
    - Tags
    - Category
    - Language
'''

# Import required libraries
from googleapiclient.discovery import build  # Google API client for YouTube API
import pandas as pd  # Data manipulation and CSV export
import isodate  # Parse ISO 8601 duration format (e.g., PT4M13S)
import datetime  # Timestamp generation for file naming
import logging


# ===== DATABRICKS CONFIGURATION =====
# In Databricks, use dbutils for secrets management and file operations
# Secrets should be stored in Databricks Secret Scopes
# Example: dbutils.secrets.put(scope="youtube-api", key="api-key", value="YOUR_API_KEY")

try:
    # Try to get dbutils (available in Databricks notebooks)
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    IS_DATABRICKS = True
except ImportError:
    # Fallback for testing outside Databricks
    IS_DATABRICKS = False
    print("Warning: Not running in Databricks environment. Using fallback configuration.")


# ===== LOGGING CONFIGURATION =====
# Set up logging for Databricks environment
# Logs will be visible in the notebook output and cluster logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ===== DBFS PATH CONFIGURATION =====
# Databricks File System (DBFS) paths for storing data
# Use /dbfs/ prefix for local file system access or dbfs:/ for Spark operations
DBFS_BASE_PATH = '/Workspace/Users/shamanthkrishna0@gmail.com/youtube-analytics-data-pipeline'
DBFS_OUTPUT_PATH = f'{DBFS_BASE_PATH}/Output'
DBFS_LOGS_PATH = f'{DBFS_BASE_PATH}/Logs'
DBFS_INPUT_PATH = f'{DBFS_BASE_PATH}/input'

def setup_dbfs_directories():
    """
    Create necessary DBFS directories for the pipeline.
    Uses dbutils.fs for Databricks-native file operations.
    """
    if IS_DATABRICKS:
        # Create directories using dbutils
        for path in [DBFS_OUTPUT_PATH, DBFS_LOGS_PATH, DBFS_INPUT_PATH]:
            dbfs_path = path.replace('/dbfs', 'dbfs:')
            try:
                dbutils.fs.mkdirs(dbfs_path)
                logger.info(f"Created/verified directory: {dbfs_path}")
            except Exception as e:
                logger.warning(f"Could not create directory {dbfs_path}: {e}")
    else:
        # Fallback for local testing
        import os
        for path in [DBFS_OUTPUT_PATH, DBFS_LOGS_PATH, DBFS_INPUT_PATH]:
            os.makedirs(path, exist_ok=True)


def get_api_credentials():
    """
    Retrieve API credentials from Databricks secrets.
    
    In Databricks, create a secret scope and add your API key:
    - Scope name: 'youtube-api' (or customize)
    - Key name: 'api-key'
    
    Command to create secret scope (run in Databricks notebook):
    dbutils.secrets.help()
    
    Returns:
        tuple: (API_KEY, S3_BUCKET_NAME)
    """
    if IS_DATABRICKS:
        try:
            # Retrieve API key from Databricks secrets
            # Replace 'youtube-api' with your actual secret scope name
            # API_KEY = dbutils.secrets.get(scope="youtube-api", key="api-key")
            API_KEY = 'AIzaSyCbTJYPS1QRlT-C6yVTgP8nwkEFNZmH7IE'

            # Optional: Get S3 bucket name from secrets
            try:
                # S3_BUCKET = dbutils.secrets.get(scope="youtube-api", key="s3-bucket")
                S3_BUCKET = 'youtube-analytics-skb'
            except:
                S3_BUCKET = None
                logger.warning("S3 bucket name not found in secrets")
            
            logger.info("Successfully retrieved credentials from Databricks secrets")
            return API_KEY, S3_BUCKET
        except Exception as e:
            logger.error(f"Error retrieving secrets: {e}")
            raise ValueError("Failed to retrieve API credentials from Databricks secrets. "
                           "Please configure the 'youtube-api' secret scope.")
    else:
        # Fallback for local testing
        import os
        from dotenv import load_dotenv
        load_dotenv()
        # API_KEY = os.getenv('YOUTUBE_API_KEY')
        API_KEY = 'AIzaSyCbTJYPS1QRlT-C6yVTgP8nwkEFNZmH7IE'
        # S3_BUCKET = os.getenv('S3_BUCKET_NAME')
        S3_BUCKET = 'youtube-analytics-skb'

        return API_KEY, S3_BUCKET


def get_channel_stats(youtube, channel_id):
    """
    Fetch channel-level statistics from YouTube API.
    
    This function retrieves basic information and statistics for a YouTube channel,
    including subscriber count, total views, and number of videos published.
    
    Parameters:
        youtube: The YouTube API client object
        channel_id (str): The unique identifier for the YouTube channel
    
    Returns:
        dict: A dictionary containing channel statistics with keys:
              - channel_id: The channel's unique identifier
              - channel_name: The display name of the channel
              - subscribers: Total subscriber count
              - total_views: Cumulative view count across all videos
              - video_count: Total number of videos published
    """
    request = youtube.channels().list(
        part='snippet,statistics',
        id=channel_id
    )
    
    response = request.execute()
    data = {}
    
    for item in response['items']:
        data['channel_id'] = item['id']
        data['channel_name'] = item['snippet']['title']
        data['subscribers'] = item['statistics'].get('subscriberCount', 0)
        data['total_views'] = item['statistics'].get('viewCount', 0)
        data['video_count'] = item['statistics'].get('videoCount', 0)
    
    return data


def get_video_ids(youtube, channel_id):
    """
    Retrieve a list of video IDs from a YouTube channel.
    
    This function searches for all videos published by a specific channel
    and returns their unique video IDs. Currently limited to 50 results
    due to API quota constraints.
    
    Parameters:
        youtube: The YouTube API client object
        channel_id (str): The unique identifier for the YouTube channel
    
    Returns:
        list: A list of video ID strings (max 50 videos)
    
    Note:
        To retrieve more than 50 videos, pagination using 'nextPageToken'
        would need to be implemented.
    """
    video_ids = []
    
    request = youtube.search().list(
        part='id',
        channelId=channel_id,
        maxResults=50,
        type='video'
    )
    
    response = request.execute()
    
    for item in response['items']:
        video_ids.append(item['id']['videoId'])
    
    return video_ids


def get_video_details(youtube, video_ids):
    """
    Fetch detailed information for multiple YouTube videos.
    
    This function retrieves comprehensive metadata and statistics for a list
    of video IDs. It processes videos in batches of 50 (API limit) to handle
    large numbers of videos efficiently.
    
    Parameters:
        youtube: The YouTube API client object
        video_ids (list): List of video ID strings to fetch details for
    
    Returns:
        list: A list of dictionaries, where each dictionary contains:
              - video_id: Unique video identifier
              - title: Video title
              - published_date: Date and time the video was published (ISO format)
              - view_count: Number of views
              - like_count: Number of likes
              - comment_count: Number of comments
              - duration: Video length in seconds (converted from ISO 8601)
              - tags: Comma-separated list of video tags
              - category_id: YouTube category identifier
              - language: Video language code
    """
    all_video_info = []
    
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(video_ids[i:i+50])
        )

        response = request.execute()
        
        for item in response['items']:
            video_info = {
                'video_id': item['id'],
                'title': item['snippet']['title'],
                'published_date': item['snippet']['publishedAt'],
                'view_count': item['statistics'].get('viewCount', 0),
                'like_count': item['statistics'].get('likeCount', 0),
                'comment_count': item['statistics'].get('commentCount', 0),
                'duration': isodate.parse_duration(item['contentDetails']['duration']).total_seconds(),
                'tags': ','.join(item['snippet'].get('tags', [])),
                'category_id': item['snippet'].get('categoryId', ''),
                'language': item['snippet'].get('defaultAudioLanguage', item['snippet'].get('defaultLanguage', ''))
            }
            all_video_info.append(video_info)
    
    return all_video_info


def upload_to_s3_from_databricks(local_file_path, s3_bucket):
    """
    Upload files to S3 from Databricks using AWS credentials.
    
    In Databricks, AWS credentials can be configured at cluster level or
    retrieved from secrets for S3 access.
    
    Parameters:
        local_file_path (str): Local DBFS path to the file
        s3_bucket (str): S3 bucket name
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        # Get AWS credentials from Databricks secrets
        if IS_DATABRICKS:
            try:
                # Use AWS credentials from widget parameters or secrets
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
            except:
                # If secrets not configured, try using instance profile/IAM role
                s3_client = boto3.client('s3')
        else:
            s3_client = boto3.client('s3')
        
        # Extract filename and create S3 key with hierarchical structure
        filename = local_file_path.split('/')[-1]
        now = datetime.datetime.now()
        s3_key = f"youtube-raw-data/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/{filename}"
        
        # Upload file to S3
        s3_client.upload_file(local_file_path, s3_bucket, s3_key)
        
        s3_uri = f"s3://{s3_bucket}/{s3_key}"
        logger.info(f"Successfully uploaded to {s3_uri}")
        print(f"✓ Uploaded: {s3_uri}")
        
        return True
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        print(f"✗ Failed to upload {local_file_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during S3 upload: {e}")
        print(f"✗ Error: {e}")
        return False


def save_to_delta_table(df, table_name, mode='append'):
    """
    Save DataFrame to Delta Lake table (Databricks native format).
    
    Delta Lake provides ACID transactions, schema enforcement, and time travel.
    This is the recommended storage format in Databricks.
    
    Parameters:
        df (pandas.DataFrame or pyspark.sql.DataFrame): Data to save
        table_name (str): Name of the Delta table
        mode (str): Save mode - 'overwrite', 'append', 'error', 'ignore'
    """
    if IS_DATABRICKS:
        try:
            # Convert pandas DataFrame to Spark DataFrame if needed
            if isinstance(df, pd.DataFrame):
                spark_df = spark.createDataFrame(df)
            else:
                spark_df = df
            
            # Write to Delta table
            spark_df.write \
                .format('delta') \
                .mode(mode) \
                .saveAsTable(table_name)
            
            logger.info(f"Successfully saved to Delta table: {table_name}")
            print(f"✓ Data saved to Delta table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to save to Delta table: {e}")
            print(f"✗ Error saving to Delta table: {e}")
    else:
        logger.warning("Delta table save skipped - not in Databricks environment")


def main(channel_id, youtube):
    """
    Main orchestration function to extract and save YouTube channel data.
    
    This function coordinates the entire data extraction pipeline:
    1. Fetch channel-level statistics
    2. Retrieve all video IDs from the channel
    3. Get detailed information for each video
    4. Return data for consolidation
    
    Parameters:
        channel_id (str): The unique identifier for the YouTube channel
        youtube: The YouTube API client object
    
    Returns:
        tuple: (channel_stats, video_details) - Dictionary and list containing
               channel statistics and video details respectively
    """
    # STEP 1: Get channel-level statistics
    logger.info(f"Fetching channel statistics for {channel_id}...")
    print(f"Fetching channel statistics for {channel_id}...")
    channel_stats = get_channel_stats(youtube, channel_id)
    
    # Display channel information
    logger.info(f"Channel: {channel_stats['channel_name']}")
    print(f"Channel: {channel_stats['channel_name']}")
    logger.info(f"Subscribers: {channel_stats['subscribers']}")
    print(f"Subscribers: {channel_stats['subscribers']}")
    logger.info(f"Total Views: {channel_stats['total_views']}")
    print(f"Total Views: {channel_stats['total_views']}")
    logger.info(f"Video Count: {channel_stats['video_count']}\n")
    print(f"Video Count: {channel_stats['video_count']}\n")
    
    # STEP 2: Get list of all video IDs from the channel
    logger.info("Fetching video IDs...")
    print("Fetching video IDs...")
    video_ids = get_video_ids(youtube, channel_id)
    logger.info(f"Found {len(video_ids)} videos\n")
    print(f"Found {len(video_ids)} videos\n")
    
    # STEP 3: Get detailed information for each video
    logger.info("Fetching video details...")
    print("Fetching video details...")
    video_details = get_video_details(youtube, video_ids)
    logger.info(f"Retrieved details for {len(video_details)} videos\n")
    print(f"Retrieved details for {len(video_details)} videos\n")
    
    # STEP 4: Enrich video details with channel information
    for video in video_details:
        video['channel_id'] = channel_stats['channel_id']
        video['channel_name'] = channel_stats['channel_name']
    
    return channel_stats, video_details


def run_ingestion_pipeline(channel_csv_path=None, save_format='csv', upload_to_s3=True):
    """
    Execute the complete YouTube data ingestion pipeline for Databricks.
    
    This is the main entry point for the Databricks notebook.
    
    Parameters:
        channel_csv_path (str): Path to CSV file with channel IDs (DBFS path)
                               If None, uses default location
        save_format (str): Output format - 'csv', 'delta', or 'both'
        upload_to_s3 (bool): Whether to upload CSV files to S3
    
    Returns:
        tuple: (channel_df, videos_df) - DataFrames with collected data
    """
    # Setup directories
    setup_dbfs_directories()
    
    # Get API credentials
    API_KEY, S3_BUCKET = get_api_credentials()
    
    # Build YouTube API client
    YOUTUBE_API_SERVICE_NAME = 'youtube'
    YOUTUBE_API_VERSION = 'v3'
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
    
    # Load channel IDs from CSV
    if channel_csv_path is None:
        # Default location in DBFS
        channel_csv_path = f'{DBFS_INPUT_PATH}/top10channelid.csv'
    
    logger.info(f"Loading channel IDs from: {channel_csv_path}")
    print(f"Loading channel IDs from: {channel_csv_path}")
    
    try:
        channelid_data = pd.read_csv(channel_csv_path)
    except FileNotFoundError:
        logger.error(f"Channel ID file not found: {channel_csv_path}")
        print(f"✗ Error: Channel ID file not found at {channel_csv_path}")
        print(f"Please upload your 'top10channelid.csv' file to {DBFS_INPUT_PATH}/")
        return None, None
    
    # Initialize data collection lists
    all_channel_stats = []
    all_video_details = []
    
    # Process each channel
    for index, row in channelid_data.iterrows():
        CHANNEL_ID = row['channel_id']
        logger.info(f"Processing channel ID: {CHANNEL_ID}")
        print(f"\nProcessing channel ID: {CHANNEL_ID}")
        
        channel_stats, video_details = main(CHANNEL_ID, youtube)
        
        all_channel_stats.append(channel_stats)
        all_video_details.extend(video_details)
    
    # Create DataFrames
    print("\nCreating final DataFrames...")
    logger.info("Creating final DataFrames...")
    
    channel_df = pd.DataFrame(all_channel_stats)
    videos_df = pd.DataFrame(all_video_details)
    
    # Generate timestamp
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save to CSV if requested
    if save_format in ['csv', 'both']:
        channel_filename = f'{DBFS_OUTPUT_PATH}/channel_stats_{timestamp}.csv'
        videos_filename = f'{DBFS_OUTPUT_PATH}/video_details_{timestamp}.csv'
        
        channel_df.to_csv(channel_filename, index=False)
        videos_df.to_csv(videos_filename, index=False)
        
        print(f"\nCSV files saved to:")
        print(f"  - {channel_filename}")
        print(f"  - {videos_filename}")
        logger.info(f"CSV files saved to {DBFS_OUTPUT_PATH}")
        
        # Upload to S3 if requested
        if upload_to_s3 and S3_BUCKET:
            print(f"\nUploading files to S3 bucket: {S3_BUCKET}...")
            logger.info(f"Starting S3 upload to bucket: {S3_BUCKET}")
            
            success_channel = upload_to_s3_from_databricks(channel_filename, S3_BUCKET)
            success_videos = upload_to_s3_from_databricks(videos_filename, S3_BUCKET)
            
            if success_channel and success_videos:
                print("\n✓ All files successfully uploaded to S3!")
                logger.info("All files successfully uploaded to S3")
            else:
                print("\n✗ Some files failed to upload to S3. Check logs for details.")
                logger.warning("Some S3 uploads failed")
        elif upload_to_s3 and not S3_BUCKET:
            print("\n⚠ S3 bucket not configured. Skipping S3 upload.")
            logger.warning("S3 bucket not configured, skipping S3 upload")
    
    # Save to Delta tables if requested
    if save_format in ['delta', 'both']:
        save_to_delta_table(channel_df, f'youtube_channel_stats_{timestamp}', mode='overwrite')
        save_to_delta_table(videos_df, f'youtube_video_details_{timestamp}', mode='overwrite')
    
    print("\n✓ Pipeline execution completed successfully!")
    logger.info("Pipeline execution completed")
    
    return channel_df, videos_df


# ===== DATABRICKS NOTEBOOK EXECUTION =====
# To run this in a Databricks notebook, execute:
#
# %run ./ingestion_databricks
# channel_df, videos_df = run_ingestion_pipeline(
#     save_format='both',  # Save as both CSV and Delta tables
#     upload_to_s3=True
# )
# display(channel_df)
# display(videos_df)


if __name__ == '__main__':
    # This section runs when executed as a standalone script
    print("="*60)
    print("YouTube Analytics Data Ingestion - Databricks Version")
    print("="*60)
    
    # Run the pipeline with default settings
    channel_df, videos_df = run_ingestion_pipeline(
        save_format='both',  # Save as both CSV and Delta
        upload_to_s3=True
    )
    
    if channel_df is not None and videos_df is not None:
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        print(f"Channels processed: {len(channel_df)}")
        print(f"Videos collected: {len(videos_df)}")
        print("\nChannel Statistics Preview:")
        print(channel_df.head())
        print("\nVideo Details Preview:")
        print(videos_df.head())
