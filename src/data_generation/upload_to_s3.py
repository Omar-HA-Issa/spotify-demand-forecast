import sys
from pathlib import Path
import logging
import boto3
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
    EndpointConnectionError
)

sys.path.append(str(Path(__file__).parent.parent.parent))
import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def validate_credentials():
    if config.AWS_ACCESS_KEY_ID == "YOUR_AWS_ACCESS_KEY_ID":
        raise ValueError("AWS credentials not configured. Please update AWS_ACCESS_KEY_ID in config.py")

    if config.AWS_SECRET_ACCESS_KEY == "YOUR_AWS_SECRET_ACCESS_KEY":
        raise ValueError("AWS credentials not configured. Please update AWS_SECRET_ACCESS_KEY in config.py")

    if config.S3_BUCKET_NAME == "YOUR_S3_BUCKET_NAME":
        raise ValueError("S3 bucket not configured. Please update S3_BUCKET_NAME in config.py")


def create_s3_client():
    try:
        logger.info("Creating S3 client")
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            region_name=config.AWS_REGION
        )
        return s3_client
    except NoCredentialsError:
        logger.error("AWS credentials not found")
        raise
    except PartialCredentialsError:
        logger.error("Incomplete AWS credentials")
        raise
    except Exception as e:
        logger.error(f"Error creating S3 client: {str(e)}")
        raise


def verify_bucket_exists(s3_client, bucket_name):
    try:
        logger.info(f"Verifying bucket exists: {bucket_name}")
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' is accessible")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.error(f"Bucket '{bucket_name}' does not exist")
            raise ValueError(f"S3 bucket '{bucket_name}' not found")
        elif error_code == '403':
            logger.error(f"Access denied to bucket '{bucket_name}'")
            raise ValueError(f"Access denied to S3 bucket '{bucket_name}'")
        else:
            logger.error(f"Error accessing bucket: {str(e)}")
            raise


def upload_file_to_s3(s3_client, file_path, bucket_name, s3_key):
    local_file = Path(file_path)
    if not local_file.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    file_size_mb = local_file.stat().st_size / (1024 * 1024)

    try:
        logger.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
        logger.info(f"File size: {file_size_mb:.2f} MB")

        s3_client.upload_file(Filename=str(local_file), Bucket=bucket_name, Key=s3_key)

        s3_uri = f"s3://{bucket_name}/{s3_key}"
        logger.info(f"Successfully uploaded to {s3_uri}")

        return s3_uri
    except ClientError as e:
        logger.error(f"Error uploading file to S3: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during upload: {str(e)}")
        raise


def verify_upload(s3_client, bucket_name, s3_key):
    try:
        logger.info("Verifying upload...")
        response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)

        file_size_mb = response['ContentLength'] / (1024 * 1024)
        logger.info(f"Verification successful!")
        logger.info(f"  Size: {file_size_mb:.2f} MB")
        logger.info(f"  Last Modified: {response['LastModified']}")
        logger.info(f"  ETag: {response['ETag']}")

        return {
            'size': response['ContentLength'],
            'last_modified': response['LastModified'],
            'etag': response['ETag']
        }
    except ClientError as e:
        logger.error(f"Error verifying upload: {str(e)}")
        raise


def main():
    try:
        logger.info("=== Starting S3 Upload ===")

        validate_credentials()
        s3_client = create_s3_client()
        verify_bucket_exists(s3_client, config.S3_BUCKET_NAME)

        s3_key = config.S3_PREFIX + Path(config.OUTPUT_PATH).name

        s3_uri = upload_file_to_s3(
            s3_client=s3_client,
            file_path=config.OUTPUT_PATH,
            bucket_name=config.S3_BUCKET_NAME,
            s3_key=s3_key
        )

        verify_upload(s3_client, config.S3_BUCKET_NAME, s3_key)

        logger.info("\n=== Upload Complete ===")
        logger.info(f"S3 URI: {s3_uri}")
        logger.info(f"Console URL: https://s3.console.aws.amazon.com/s3/object/{config.S3_BUCKET_NAME}?prefix={s3_key}")

    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        logger.error("Please update config.py with your AWS credentials and S3 bucket name")
        sys.exit(1)

    except FileNotFoundError as e:
        logger.error(f"File error: {str(e)}")
        logger.error("Please run generate_data.py first to create the streaming data file")
        sys.exit(1)

    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"Credentials error: {str(e)}")
        logger.error("Please check your AWS credentials in config.py")
        sys.exit(1)

    except EndpointConnectionError as e:
        logger.error(f"Network error: {str(e)}")
        logger.error("Could not connect to AWS. Please check your internet connection")
        sys.exit(1)

    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"AWS error ({error_code}): {str(e)}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
