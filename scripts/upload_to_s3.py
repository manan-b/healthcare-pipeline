#!/usr/bin/env python3
"""
upload_to_s3.py
---------------
Utility script to upload patient_records.csv to your S3 bucket.

Usage:
  python scripts/upload_to_s3.py --file <path/to/patient_records.csv> --bucket <your-bucket-name>

Example:
  python scripts/upload_to_s3.py --file ../data/sample/patient_records.csv --bucket revature-health-data
"""

import boto3
import argparse
import os
import sys

def upload_file_to_s3(local_file_path: str, bucket_name: str, s3_key: str = None):
    """
    Upload a local file to Amazon S3.

    Args:
        local_file_path : Absolute or relative path to the local file.
        bucket_name     : Target S3 bucket name (without s3:// prefix).
        s3_key          : Destination key in S3 (defaults to raw/<filename>).
    """
    if not os.path.exists(local_file_path):
        print(f"ERROR: File not found: {local_file_path}")
        sys.exit(1)

    filename = os.path.basename(local_file_path)
    if s3_key is None:
        s3_key = f"raw/{filename}"

    s3_client = boto3.client("s3")

    file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
    print(f"Uploading: {local_file_path}  ({file_size_mb:.1f} MB)")
    print(f"Target   : s3://{bucket_name}/{s3_key}")

    s3_client.upload_file(local_file_path, bucket_name, s3_key)

    print(f"Upload successful!")
    print(f"S3 path  : s3://{bucket_name}/{s3_key}")
    print(f"\nVerifying...")
    obj = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
    print(f"  Size on S3: {obj['ContentLength']:,} bytes")
    print(f"  ETag      : {obj['ETag']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload a file to AWS S3")
    parser.add_argument("--file",   required=True, help="Path to the local file to upload")
    parser.add_argument("--bucket", required=True, help="S3 bucket name (no s3:// prefix)")
    parser.add_argument("--key",    default=None,  help="S3 key/path (default: raw/<filename>)")
    args = parser.parse_args()

    upload_file_to_s3(args.file, args.bucket, args.key)
