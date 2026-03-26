import pandas as pd
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

KeyID = os.getenv("KeyID")
AccessKey = os.getenv("AccessKey")


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "af-south-1"),
        aws_access_key_id= KeyID,
        aws_secret_access_key= AccessKey
    )

def extract_from_s3(bucket_name, file_key, chunk_size=5000):
    s3 = get_s3_client()

    obj = s3.get_object(Bucket=bucket_name, Key=file_key)

    # Stream CSV in chunks
    return pd.read_csv(obj['Body'], chunksize=chunk_size)

if not KeyID or not AccessKey:
    raise ValueError("Values not set in .env file")



#if __name__ == "__main__":
#    bucket = "govdata-pipeline-extract"
#    key = "GovFY25_Q3.csv"
#
#    for chunk in extract_from_s3(bucket, key):
#        clean_chunk = transform_chunk(chunk)
#
#        print(clean_chunk.head())
#        break
#
#    for chunk in extract_from_s3(bucket, key):
#        print(chunk.shape)
#        break