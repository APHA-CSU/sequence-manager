import boto3
import botocore

s3 = boto3.resource('s3')

def s3_object_exists(bucket, key):
    """
        Returns true if the S3 key in the S3 bucket. False otherwise
        https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
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
