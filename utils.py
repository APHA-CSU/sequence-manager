import boto3
import botocore
import subprocess

s3 = boto3.resource('s3')

def s3_object_exists(bucket, key):
    """
        Returns true if the S3 key in the S3 bucket. False otherwise
        Thanks: https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    """
    
    key_exists = True

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

def s3_sync(src_dir, bucket, key):
    """
        Synchronise src_dir to s3://{bucket}/{key}
    """
    target_uri = f's3://{bucket}/{key}'

    # Make sure the key exists 
    if s3_object_exists(bucket, key):
        raise Exception(f'{target_uri} already exists')

    # Sync
    return_code = subprocess.run([
        "aws", 's3', 'sync', src_dir, target_uri
    ]).returncode

    if return_code:
        raise Exception('aws s3 sync failed: %s'%(return_code))

if __name__ == '__main__':
    print('key exists: ', s3_object_exists('s3-csu-003', 'aaron/'))