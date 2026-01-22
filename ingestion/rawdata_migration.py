"""
S3 Raw Data Migration Script

This script handles uploading ingested YouTube data to AWS S3 with a hierarchical folder structure.
Files are organized by timestamp (year/month/day/hour) for easy data lake management and partitioning.

Purpose:
    - Automatically parse timestamps from CSV filenames
    - Create organized folder hierarchy in S3 for data lake best practices
    - Enable easy data querying by date/time partitions in Databricks/Athena
    - Handle AWS authentication and upload errors gracefully

Data Lake Structure:
    s3://bucket-name/youtube-raw-data/YYYY/MM/DD/HH/filename.csv
    
    Example:
    s3://my-bucket/youtube-raw-data/2026/01/19/21/video_details_20260119_213221.csv
"""

# ===== IMPORT REQUIRED LIBRARIES =====
import boto3  # AWS SDK for Python - handles S3 interactions
import os  # Operating system utilities - file path operations
import re  # Regular expressions - pattern matching for filename parsing
import logging  # Logging framework - track script execution and errors
from botocore.exceptions import NoCredentialsError, PartialCredentialsError  # AWS credential errors


def parse_timestamp_from_filename(filename):
    """
    Parse timestamp from CSV filename and extract date/time components.
    
    This function uses regex to extract the timestamp embedded in the filename.
    It breaks down the timestamp into individual components (year, month, day, hour, etc.)
    which are then used to create the hierarchical folder structure in S3.
    
    Expected filename format: 
        - channel_stats_20260119_213221.csv 
        - video_details_20260119_213221.csv
        
    Timestamp format: YYYYMMDD_HHMMSS
        - YYYY = 4-digit year
        - MM = 2-digit month (01-12)
        - DD = 2-digit day (01-31)
        - HH = 2-digit hour (00-23)
        - MM = 2-digit minute (00-59)
        - SS = 2-digit second (00-59)
    
    Parameters:
        filename (str): The CSV filename to parse (e.g., "video_details_20260119_213221.csv")
    
    Returns:
        dict: Dictionary containing parsed timestamp components:
              {'year': '2026', 'month': '01', 'day': '19', 'hour': '21', 'minute': '32', 'second': '21'}
        None: If filename doesn't match the expected pattern
    
    Example:
        Input: "video_details_20260119_213221.csv"
        Output: {'year': '2026', 'month': '01', 'day': '19', 'hour': '21', 'minute': '32', 'second': '21'}
    """
    # ===== DEFINE REGEX PATTERN =====
    # Regular expression to extract timestamp from filename
    # Pattern breakdown:
    #   .*_           - Match anything followed by underscore (e.g., "video_details_")
    #   (\d{4})       - Capture group 1: exactly 4 digits (YYYY - year)
    #   (\d{2})       - Capture group 2: exactly 2 digits (MM - month)
    #   (\d{2})       - Capture group 3: exactly 2 digits (DD - day)
    #   _             - Literal underscore separator
    #   (\d{2})       - Capture group 4: exactly 2 digits (HH - hour)
    #   (\d{2})       - Capture group 5: exactly 2 digits (MM - minute)
    #   (\d{2})       - Capture group 6: exactly 2 digits (SS - second)
    #   \.csv         - Literal .csv extension
    pattern = r'.*_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.csv'
    
    # ===== ATTEMPT TO MATCH PATTERN =====
    # Try to match the pattern against the filename
    match = re.match(pattern, filename)
    
    if match:
        # ===== SUCCESSFUL MATCH - EXTRACT COMPONENTS =====
        # Extract timestamp components from the matched groups
        # Each group() call retrieves a captured group from the regex match
        return {
            'year': match.group(1),    # Group 1: YYYY (e.g., '2026')
            'month': match.group(2),   # Group 2: MM (e.g., '01')
            'day': match.group(3),     # Group 3: DD (e.g., '19')
            'hour': match.group(4),    # Group 4: HH (e.g., '21')
            'minute': match.group(5),  # Group 5: MM (e.g., '32')
            'second': match.group(6)   # Group 6: SS (e.g., '21')
        }
    else:
        # ===== PATTERN MISMATCH - LOG ERROR =====
        # Filename doesn't match expected pattern, log error for debugging
        error_msg = f"Filename {filename} doesn't match expected pattern (expected: *_YYYYMMDD_HHMMSS.csv)"
        logging.error(error_msg)
        return None


def build_s3_path(filename, base_prefix='youtube-raw-data'):
    """
    Build hierarchical S3 path from filename timestamp.
    
    This function creates an organized folder structure in S3 based on the timestamp
    embedded in the filename. This hierarchical structure enables:
    1. Easy data partitioning for query optimization
    2. Simple data retention policies (e.g., delete old months)
    3. Clear data organization for data lake management
    4. Efficient querying by date/time ranges in Databricks/Athena
    
    S3 Path Structure:
        base_prefix/YYYY/MM/DD/HH/filename.csv
        
    Example paths:
        youtube-raw-data/2026/01/19/21/video_details_20260119_213221.csv
        youtube-raw-data/2026/01/19/21/channel_stats_20260119_213221.csv
    
    Parameters:
        filename (str): The CSV filename containing timestamp
        base_prefix (str): Base folder name in S3 bucket (default: 'youtube-raw-data')
                          This acts as the root folder for all YouTube data
    
    Returns:
        str: Complete S3 object key (full path in bucket)
        None: If timestamp cannot be parsed from filename
    
    Example:
        Input: filename = "video_details_20260119_213221.csv", base_prefix = "youtube-raw-data"
        Output: "youtube-raw-data/2026/01/19/21/video_details_20260119_213221.csv"
    """
    # ===== PARSE TIMESTAMP FROM FILENAME =====
    # Extract date/time components using the parse function
    timestamp_parts = parse_timestamp_from_filename(filename)
    
    # ===== CHECK IF PARSING WAS SUCCESSFUL =====
    if not timestamp_parts:
        # Parsing failed - filename doesn't match expected format
        # Return None to signal failure to calling function
        return None
    
    # ===== BUILD HIERARCHICAL S3 PATH =====
    # Construct the full S3 object key using the timestamp components
    # Format: base_prefix/year/month/day/hour/filename
    # This creates a partition-friendly structure for big data tools
    s3_path = f"{base_prefix}/{timestamp_parts['year']}/{timestamp_parts['month']}/{timestamp_parts['day']}/{timestamp_parts['hour']}/{filename}"
    
    # Return the complete S3 path
    return s3_path



def upload_to_s3(local_file_path, bucket_name, s3_key):
    """
    Upload a file to AWS S3 bucket with comprehensive error handling.
    
    This function handles the actual file transfer to S3. It uses boto3 (AWS SDK)
    to upload the file and provides detailed error handling for common issues like:
    - Missing files
    - Invalid AWS credentials
    - Network errors
    - Permission issues
    
    Authentication:
        The function uses AWS credentials from one of these sources (in order):
        1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        2. AWS credentials file (~/.aws/credentials)
        3. IAM role (if running on AWS EC2/Lambda)
    
    Parameters:
        local_file_path (str): Path to the local file to upload
                              Example: "Output/video_details_20260119_213221.csv"
        bucket_name (str): Name of the target S3 bucket
                          Example: "my-youtube-data-lake"
        s3_key (str): S3 object key (full path within the bucket)
                     Example: "youtube-raw-data/2026/01/19/21/video_details_20260119_213221.csv"
    
    Returns:
        bool: True if upload successful, False otherwise
    
    Example:
        success = upload_to_s3(
            "Output/video_details_20260119_213221.csv",
            "my-data-lake",
            "youtube-raw-data/2026/01/19/21/video_details_20260119_213221.csv"
        )
    """
    # ===== INITIALIZE AWS S3 CLIENT =====
    # Create a boto3 S3 client object
    # This client will handle all interactions with AWS S3 service
    # Credentials are automatically loaded from:
    #   1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    #   2. AWS credentials file (~/.aws/credentials)
    #   3. IAM instance role (if running on EC2)
    s3_client = boto3.client('s3')

    try:
        # ===== UPLOAD FILE TO S3 =====
        # The upload_file() method handles the file transfer
        # It automatically handles multipart uploads for large files
        # Parameters:
        #   - local_file_path: Source file on local disk
        #   - bucket_name: Target S3 bucket
        #   - s3_key: Destination path within the bucket
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        
        # ===== LOG SUCCESS =====
        # Create a user-friendly success message
        # Show both local path and S3 URI for confirmation
        success_msg = f"✓ Uploaded: {local_file_path} -> s3://{bucket_name}/{s3_key}"
        print(success_msg)  # Display to console
        logging.info(success_msg)  # Record in log file
        
        return True  # Indicate successful upload
        
    except FileNotFoundError:
        # ===== HANDLE MISSING FILE ERROR =====
        # This occurs when the specified local file doesn't exist
        # Possible causes:
        #   - File was deleted between CSV creation and upload
        #   - Incorrect file path provided
        #   - File is in a different directory
        error_msg = f"✗ File not found: {local_file_path}"
        print(error_msg)
        logging.error(error_msg)
        return False
        
    except NoCredentialsError:
        # ===== HANDLE MISSING AWS CREDENTIALS =====
        # This occurs when boto3 cannot find AWS credentials
        # Solutions:
        #   - Run 'aws configure' to set up credentials
        #   - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
        #   - Add credentials to .env file
        error_msg = "✗ AWS credentials not found. Configure using 'aws configure' or environment variables."
        print(error_msg)
        logging.error(error_msg)
        return False
        
    except PartialCredentialsError:
        # ===== HANDLE INCOMPLETE AWS CREDENTIALS =====
        # This occurs when some but not all credential components are provided
        # Example: Access key provided but secret key is missing
        error_msg = "✗ Incomplete AWS credentials provided. Need both access key and secret key."
        print(error_msg)
        logging.error(error_msg)
        return False
        
    except Exception as e:
        # ===== HANDLE ALL OTHER ERRORS =====
        # Catch-all for any unexpected errors:
        #   - Network connectivity issues
        #   - S3 bucket doesn't exist
        #   - Permission denied (IAM policy issues)
        #   - Invalid bucket name
        #   - S3 service unavailable
        error_msg = f"✗ Upload failed: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        return False


def export_raw_data_to_s3(local_file_path, bucket_name, base_prefix='youtube-raw-data'):
    """
    Main function to export CSV file to S3 with hierarchical folder structure.
    
    This is the primary function called from the ingestion script. It orchestrates
    the entire S3 upload process by:
    1. Extracting the filename from the full path
    2. Building the hierarchical S3 path based on timestamp
    3. Uploading the file to the constructed S3 location
    
    The function automatically creates a data lake-friendly folder structure that
    enables efficient querying and data management in tools like:
    - Databricks
    - AWS Athena
    - AWS Glue
    - Apache Spark
    
    S3 Folder Structure Created:
        s3://bucket-name/youtube-raw-data/YYYY/MM/DD/HH/filename.csv
        
    Benefits of this structure:
        - Easy to query by date/time ranges
        - Enables partition pruning in big data engines
        - Simplifies data retention policies
        - Provides clear data organization
    
    Parameters:
        local_file_path (str): Full path to the local CSV file to upload
                              Example: "Output/video_details_20260119_213221.csv"
                              Can be absolute or relative path
        
        bucket_name (str): Name of the target S3 bucket (must already exist)
                          Example: "my-youtube-data-lake"
                          
        base_prefix (str): Root folder in S3 for organizing YouTube data
                          Default: 'youtube-raw-data'
                          This acts as a namespace to separate different data sources
    
    Returns:
        bool: True if file was successfully uploaded to S3
              False if any error occurred (file not found, invalid path, upload failed)
    
    Example Usage:
        # Upload a single file
        success = export_raw_data_to_s3(
            local_file_path="Output/video_details_20260119_213221.csv",
            bucket_name="my-data-lake",
            base_prefix="youtube-raw-data"
        )
        
        if success:
            print("File uploaded successfully!")
        else:
            print("Upload failed - check logs")
    
    Result Example:
        Input: local_file_path = "Output/video_details_20260119_213221.csv"
               bucket_name = "my-data-lake"
               base_prefix = "youtube-raw-data"
        
        Output: File uploaded to:
                s3://my-data-lake/youtube-raw-data/2026/01/19/21/video_details_20260119_213221.csv
    """
    # ===== EXTRACT FILENAME FROM FULL PATH =====
    # os.path.basename() extracts just the filename from the full path
    # Example: "Output/video_details_20260119_213221.csv" -> "video_details_20260119_213221.csv"
    # This works with both absolute and relative paths
    filename = os.path.basename(local_file_path)
    
    # ===== BUILD HIERARCHICAL S3 PATH =====
    # Use the build_s3_path() function to create the organized folder structure
    # This function parses the timestamp from the filename and builds:
    # base_prefix/YYYY/MM/DD/HH/filename.csv
    s3_key = build_s3_path(filename, base_prefix)
    
    # ===== VALIDATE S3 PATH CREATION =====
    # If build_s3_path() returns None, the filename doesn't match expected pattern
    if not s3_key:
        # Log error and notify user
        error_msg = f"✗ Cannot create S3 path for file: {filename} (invalid filename format)"
        print(error_msg)
        logging.error(error_msg)
        return False  # Signal failure
    
    # ===== UPLOAD FILE TO S3 =====
    # Call the upload function with the local file path, bucket name, and constructed S3 key
    # The upload_to_s3() function handles all AWS interactions and error handling
    return upload_to_s3(local_file_path, bucket_name, s3_key)


# ===== STANDALONE EXECUTION BLOCK =====
# This block runs only when the script is executed directly (not when imported)
# Useful for testing the S3 upload functionality independently
if __name__ == "__main__":
    # ===== CONFIGURE LOGGING FOR TESTING =====
    # Set up basic logging configuration for standalone execution
    # This ensures log messages are visible when testing
    logging.basicConfig(
        level=logging.INFO,  # Show INFO level and above
        format='%(asctime)s - %(levelname)s - %(message)s'  # Timestamp - Level - Message
    )
    
    # ===== EXAMPLE CONFIGURATION FOR TESTING =====
    # These are placeholder values - replace with your actual values for testing
    local_file = "Output/video_details_20260119_213221.csv"  # Path to test file
    bucket = "your-s3-bucket-name"  # Your S3 bucket name
    
    # ===== EXECUTE TEST UPLOAD =====
    # Try uploading the test file to S3
    print("Starting test upload to S3...")
    success = export_raw_data_to_s3(local_file, bucket)
    
    # ===== DISPLAY TEST RESULTS =====
    if success:
        print("\n✓ Test upload completed successfully!")
    else:
        print("\n✗ Test upload failed - check error messages above")