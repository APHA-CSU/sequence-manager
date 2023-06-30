from logging import StreamHandler, Handler, FileHandler
from pathlib import Path

import boto3

class S3LoggingHandler(FileHandler):

    def __init__(self, filename, bucket, key, endpoint_url=None):
        """
            This custom logger logs events to a file and uploads to S3
        """
        # Initialise class like a normal FileHandler does
        super().__init__(filename)

        # S3 Target
        self.bucket = bucket
        self.key = key

        # Endpoint Url is required to transfer from Weybridge to the SCE
        # However it is not required for transfers within the SCE
        if endpoint_url is None:
            self.s3 = boto3.client("s3")
        else:
            self.s3 = boto3.client("s3", endpoint_url=endpoint_url)

    def emit(self, record):
        """
            Logs file locally and then upload to s3
        """
        # Log like a normal FileHandler does
        super().emit(record)

        # Upload to S3
        self.s3.upload_file(self.baseFilename, self.bucket, self.key)
