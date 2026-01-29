'''
YouTube Analytics Data Ingestion Script

This script extracts comprehensive data from YouTube channels using the YouTube Data API v3.

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
import os  # Access environment variables
from dotenv import load_dotenv  # Load environment variables from .env file
import logging
from rawdata_migration import export_raw_data_to_s3  # S3 upload functionality


# ===== LOGGING CONFIGURATION =====
# Set up logging to track script execution and debug issues
# Logs are stored in a separate 'Logs' folder with timestamped filenames
# Get the project root directory (parent of ingestion folder)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
log_folder = os.path.join(project_root, 'Logs')

# Create Logs directory if it doesn't exist
# exist_ok=True prevents error if folder already exists from previous runs
os.makedirs(log_folder, exist_ok=True)

# ===== OUTPUT FOLDER CONFIGURATION =====
# Set up output folder where CSV files will be saved
# This keeps the project directory organized by separating raw data from code
output_folder = os.path.join(project_root, 'Output')

# Create Output directory for CSV files if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Generate a unique log filename with current timestamp
# Format: log_20260119_143025.log (YYYYMMDD_HHMMSS)
# This allows tracking execution history across multiple runs
log_filename = os.path.join(log_folder, f'log_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

# Configure the logging system
logging.basicConfig(
    filename=log_filename,  # Write logs to the timestamped file
    level=logging.INFO,  # Capture INFO level and above (INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Format: timestamp - level - message
)


# Load environment variables from .env file (contains API key)
load_dotenv()

# Retrieve YouTube API key from environment variables
API_KEY = os.getenv('YOUTUBE_API_KEY')

# YouTube API service configuration
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

# Build the YouTube API client object
# This object will be used to make all API requests
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
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
    # Create API request for channel information
    # 'snippet' part contains basic channel info (name, description, etc.)
    # 'statistics' part contains metrics (subscribers, views, video count)
    request = youtube.channels().list(
        part='snippet,statistics',
        id=channel_id
    )
    
    # Execute the API request and get the response
    response = request.execute()
    
    # Initialize empty dictionary to store channel data
    data = {}
    
    # Parse the response and extract relevant fields
    # The API returns a list of items (usually just one for a single channel ID)
    for item in response['items']:
        data['channel_id'] = item['id']
        data['channel_name'] = item['snippet']['title']
        # Use .get() with default value 0 in case the field is missing
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
    # Initialize empty list to store video IDs
    video_ids = []
    
    # Create search request to find all videos from the channel
    # 'part=id' means we only want the video IDs (minimal data to save quota)
    request = youtube.search().list(
        part='id',
        channelId=channel_id,  # Filter results to this specific channel
        maxResults=50,  # Maximum allowed per request (API limit)
        type='video'  # Only return videos (exclude playlists, channels)
    )
    
    # Execute the API request
    response = request.execute()
    
    # Extract video IDs from the response
    # Each item in the response contains an 'id' object with a 'videoId' field
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
    # Initialize list to store all video information
    all_video_info = []
    
    # Process videos in batches of 50 (YouTube API allows max 50 IDs per request)
    # range(start, stop, step) creates batches: 0-49, 50-99, 100-149, etc.
    for i in range(0, len(video_ids), 50):
        # Create API request for video details
        # 'snippet' contains metadata (title, description, tags, etc.)
        # 'statistics' contains metrics (views, likes, comments)
        # 'contentDetails' contains duration and other technical details
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(video_ids[i:i+50])  # Join batch of up to 50 video IDs with commas
        )

        # Execute the API request
        response = request.execute()
        
        # Parse each video in the response
        for item in response['items']:
            # Extract and structure video information
            video_info = {
                'video_id': item['id'],
                'title': item['snippet']['title'],
                'published_date': item['snippet']['publishedAt'],
                # Use .get() with default 0 in case statistics are disabled
                'view_count': item['statistics'].get('viewCount', 0),
                'like_count': item['statistics'].get('likeCount', 0),
                'comment_count': item['statistics'].get('commentCount', 0),
                # Parse ISO 8601 duration (e.g., 'PT4M13S') to total seconds
                'duration': isodate.parse_duration(item['contentDetails']['duration']).total_seconds(),
                # Join tags list into comma-separated string (empty list if no tags)
                'tags': ','.join(item['snippet'].get('tags', [])),
                'category_id': item['snippet'].get('categoryId', ''),
                # Try audio language first, fall back to default language
                'language': item['snippet'].get('defaultAudioLanguage', item['snippet'].get('defaultLanguage', ''))
            }
            # Add this video's info to the complete list
            all_video_info.append(video_info)
    
    return all_video_info

def main(channel_id):
    """
    Main orchestration function to extract and save YouTube channel data.
    
    This function coordinates the entire data extraction pipeline:
    1. Fetch channel-level statistics
    2. Retrieve all video IDs from the channel
    3. Get detailed information for each video
    4. Convert data to pandas DataFrames
    5. Export data to timestamped CSV files
    
    Parameters:
        channel_id (str): The unique identifier for the YouTube channel
    
    Returns:
        tuple: (channel_df, videos_df) - Two pandas DataFrames containing
               channel statistics and video details respectively
    """
    # STEP 1: Get channel-level statistics
    logging.info(f"Fetching channel statistics for {channel_id}...")
    print(f"Fetching channel statistics for {channel_id}...")
    channel_stats = get_channel_stats(youtube, channel_id)
    
    # Display channel information to the user
    logging.info(f"Channel: {channel_stats['channel_name']}")
    print(f"Channel: {channel_stats['channel_name']}")
    logging.info(f"Subscribers: {channel_stats['subscribers']}")
    print(f"Subscribers: {channel_stats['subscribers']}")
    logging.info(f"Total Views: {channel_stats['total_views']}")
    print(f"Total Views: {channel_stats['total_views']}")
    logging.info(f"Video Count: {channel_stats['video_count']}\n")
    print(f"Video Count: {channel_stats['video_count']}\n")
    
    # STEP 2: Get list of all video IDs from the channel
    logging.info("Fetching video IDs...")
    print("Fetching video IDs...")
    video_ids = get_video_ids(youtube, channel_id)
    logging.info(f"Found {len(video_ids)} videos\n")
    print(f"Found {len(video_ids)} videos\n")
    
    # STEP 3: Get detailed information for each video
    logging.info("Fetching video details...")
    print("Fetching video details...")
    video_details = get_video_details(youtube, video_ids)
    logging.info(f"Retrieved details for {len(video_details)} videos\n")
    print(f"Retrieved details for {len(video_details)} videos\n")
    
    # STEP 4: Enrich video details with channel information
    # Add channel_id and channel_name to each video detail dictionary
    # This is crucial for identifying which videos belong to which channel
    # when we combine data from multiple channels into a single CSV file
    for video in video_details:
        video['channel_id'] = channel_stats['channel_id']  # Add channel ID reference
        video['channel_name'] = channel_stats['channel_name']  # Add channel name for readability
    
    # Return raw data instead of saving immediately
    # This allows the main script to collect data from multiple channels
    # before creating a single consolidated CSV file
    return channel_stats, video_details


# ===== SCRIPT ENTRY POINT =====
# This block only runs when the script is executed directly (not when imported as a module)
if __name__ == '__main__':
    # ===== LOAD CHANNEL IDS FROM CSV FILE =====
    # Read the CSV file containing channel IDs to process
    # Expected CSV format: columns 'channel_name' and 'channel_id'
    # This allows batch processing of multiple channels in one script run
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'top10channelid.csv')
    channelid_data = pd.read_csv(csv_path)
    
    # Ensure output folder exists in the parent directory (project root)
    # Go up one level from ingestion/ to project root
    project_root = os.path.dirname(script_dir)
    output_folder = os.path.join(project_root, 'Output')
    os.makedirs(output_folder, exist_ok=True)
    
    # Update log folder path to project root as well
    log_folder = os.path.join(project_root, 'Logs')
    os.makedirs(log_folder, exist_ok=True)

    # ===== INITIALIZE DATA COLLECTION LISTS =====
    # These lists will accumulate data from ALL channels before saving
    # This approach creates one consolidated file instead of separate files per channel
    all_channel_stats = []  # Will store dictionaries of channel statistics
    all_video_details = []  # Will store dictionaries of video details from all channels

    # ===== PROCESS EACH CHANNEL ITERATIVELY =====
    # Loop through each row in the CSV file to process all channels
    for index, row in channelid_data.iterrows():
        # Extract channel ID from the current row
        CHANNEL_ID = row['channel_id']
        
        # Log and print which channel is being processed for tracking
        logging.info(f"Processing channel ID: {CHANNEL_ID}")
        print(f"\nProcessing channel ID: {CHANNEL_ID}")
        
        # Execute the main data extraction pipeline for this channel
        # Returns: channel_stats (dict) and video_details (list of dicts)
        channel_stats, video_details = main(CHANNEL_ID)
        
        # ===== COLLECT DATA FROM THIS CHANNEL =====
        # Append channel statistics (single dict) to the list
        all_channel_stats.append(channel_stats)
        
        # Extend (not append) video details list with all videos from this channel
        # extend() adds individual items, append() would add the entire list as one item
        all_video_details.extend(video_details)
    
    # ===== CREATE CONSOLIDATED DATAFRAMES =====
    # After processing all channels, convert collected data into pandas DataFrames
    print("\nCreating final CSV files...")
    logging.info("Creating final CSV files...")
    
    # Convert list of channel stat dictionaries into a DataFrame
    # Each row represents one channel (10 rows total for 10 channels)
    channel_df = pd.DataFrame(all_channel_stats)
    
    # Convert list of video detail dictionaries into a DataFrame
    # Each row represents one video from any channel
    # Could be 100s or 1000s of rows depending on how many videos each channel has
    videos_df = pd.DataFrame(all_video_details)
    
    # ===== GENERATE UNIQUE TIMESTAMP FOR FILE NAMING =====
    # Create timestamp in format: YYYYMMDD_HHMMSS (e.g., 20260119_143025)
    # This ensures each script run creates unique files without overwriting previous data
    # Useful for tracking data collection history and comparing changes over time
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # ===== CREATE FULL FILE PATHS =====
    # Combine output folder path with timestamped filenames
    # os.path.join handles path separators correctly across different operating systems
    channel_filename = os.path.join(output_folder, f'channel_stats_{timestamp}.csv')
    videos_filename = os.path.join(output_folder, f'video_details_{timestamp}.csv')
    
    # ===== SAVE DATAFRAMES TO CSV FILES =====
    # index=False excludes the pandas row index from the CSV output
    # This keeps the CSV clean with only the actual data columns
    channel_df.to_csv(channel_filename, index=False)
    videos_df.to_csv(videos_filename, index=False)
    
    # ===== CONFIRM SUCCESSFUL COMPLETION =====
    # Provide feedback to user and log file about where data was saved
    print(f"\nAll data saved to:")
    print(f"  - {channel_filename}")
    print(f"  - {videos_filename}")
    logging.info(f"All data saved to:")
    logging.info(f"  - {channel_filename}")
    logging.info(f"  - {videos_filename}")
    
    # ===== UPLOAD TO AWS S3 =====
    # Upload CSV files to S3 with hierarchical folder structure
    # Files will be organized as: s3://bucket/youtube-raw-data/YYYY/MM/DD/HH/filename.csv
    
    # Get S3 bucket name from environment variable
    # Add this to your .env file: S3_BUCKET_NAME=your-bucket-name
    S3_BUCKET = os.getenv('S3_BUCKET_NAME')
    
    if S3_BUCKET:
        print(f"\nUploading files to S3 bucket: {S3_BUCKET}...")
        logging.info(f"Starting S3 upload to bucket: {S3_BUCKET}")
        
        # Upload channel stats file
        success_channel = export_raw_data_to_s3(channel_filename, S3_BUCKET)
        
        # Upload video details file
        success_videos = export_raw_data_to_s3(videos_filename, S3_BUCKET)
        
        if success_channel and success_videos:
            print("\n✓ All files successfully uploaded to S3!")
            logging.info("All files successfully uploaded to S3")
        else:
            print("\n✗ Some files failed to upload to S3. Check logs for details.")
            logging.warning("Some S3 uploads failed")
    else:
        print("\n⚠ S3_BUCKET_NAME not set in .env file. Skipping S3 upload.")
        logging.warning("S3_BUCKET_NAME not configured, skipping S3 upload")

