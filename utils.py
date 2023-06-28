import json
import subprocess

import boto3
import botocore


def s3_object_exists(bucket, key, s3_endpoint_url):
    """
        Returns true if the S3 key is in the S3 bucket. False otherwise
        Thanks: https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    """

    key_exists = True

    s3 = boto3.resource('s3', endpoint_url=s3_endpoint_url)

    try:
        s3.Object(bucket, key).load()

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            key_exists = False

        else:
            # Something else has gone wrong.
            raise e

    return key_exists


def s3_sync(src_dir, bucket, key, s3_endpoint_url):
    """
        Upload src_dir to s3://{bucket}/{key}
    """
    target_uri = f's3://{bucket}/{key}'

    # Don't overwrite
    if s3_object_exists(bucket, key, s3_endpoint_url):
        raise Exception(f'{target_uri} already exists')

    # Sync
    return_code = subprocess.run([
        "aws", "s3",
        "--endpoint-url", s3_endpoint_url,
        "sync", src_dir, target_uri
    ]).returncode

    if return_code:
        raise Exception('aws s3 sync failed: %s' % (return_code))


def upload_json(bucket, key, s3_endpoint_url, dictionary,
                profile='default', indent=4):
    """
        Upload json data to s3

        bucket: S3 Bucket Name
        key: S3 key the json file is stored under
        dictionary: json serialisable python dictionary for S3 upload
        endpoint_url: S3 endpoint url
        indent: Number of indentation spaces in the json
    """
    # set the aws profile
    boto3.setup_default_session(profile_name=profile)
    s3 = boto3.resource('s3', endpoint_url=s3_endpoint_url)
    obj = s3.Object(bucket, key)

    obj.put(Body=(bytes(json.dumps(dictionary, indent=indent).encode('UTF-8'))))
    # reset the aws profile
    boto3.setup_default_session(profile_name='default')


def s3_download_file(bucket, key, dest, s3_endpoint_url):
    """
        Downloads s3 folder at the key-bucket pair (strings) to dest
        path (string)
    """
    if s3_object_exists(bucket, key, s3_endpoint_url):
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, dest)
    else:
        raise Exception(f'{key} not found in {bucket}')
