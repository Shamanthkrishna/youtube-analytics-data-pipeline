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
    print(f"Fetching channel statistics for {channel_id}...")
    channel_stats = get_channel_stats(youtube, channel_id)
    
    # Display channel information to the user
    print(f"Channel: {channel_stats['channel_name']}")
    print(f"Subscribers: {channel_stats['subscribers']}")
    print(f"Total Views: {channel_stats['total_views']}")
    print(f"Video Count: {channel_stats['video_count']}\n")
    
    # STEP 2: Get list of all video IDs from the channel
    print("Fetching video IDs...")
    video_ids = get_video_ids(youtube, channel_id)
    print(f"Found {len(video_ids)} videos\n")
    
    # STEP 3: Get detailed information for each video
    print("Fetching video details...")
    video_details = get_video_details(youtube, video_ids)
    print(f"Retrieved details for {len(video_details)} videos\n")
    
    # STEP 4: Convert data to pandas DataFrames for easy manipulation
    # Channel stats is a single row (one channel), so wrap in a list
    channel_df = pd.DataFrame([channel_stats])
    # Video details is already a list of dictionaries
    videos_df = pd.DataFrame(video_details)
    
    # STEP 5: Save data to CSV files with timestamp
    # Generate timestamp in format: YYYYMMDD_HHMMSS (e.g., 20260113_143025)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    channel_filename = f'channel_stats_{timestamp}.csv'
    videos_filename = f'video_details_{timestamp}.csv'
    
    # Export DataFrames to CSV (index=False to exclude row numbers)
    channel_df.to_csv(channel_filename, index=False)
    videos_df.to_csv(videos_filename, index=False)
    
    # Confirm successful save to user
    print(f"Data saved to:")
    print(f"  - {channel_filename}")
    print(f"  - {videos_filename}")
    
    # Return DataFrames for further analysis if needed
    return channel_df, videos_df


# Entry point of the script
# This block only runs when the script is executed directly (not when imported)
if __name__ == '__main__':
    # Set the YouTube channel ID you want to analyze
    # You can find a channel ID by:
    # 1. Going to the channel page on YouTube
    # 2. Clicking on the channel URL - it will show the ID
    # 3. Or using a channel ID finder tool
    CHANNEL_ID = 'UCX6OQ3DkcsbYNE6H8uQQuVA'  # MrBeast channel as example
    
    # Execute the main data extraction pipeline
    main(CHANNEL_ID)
