from logging import StreamHandler, Handler

import boto3  

class S3LoggingHandler(Handler):

    def __init__(self, filename, bucket, key):
        """
            This custom logger upload a file S3 on every log

            TODO: This is a bit hacky. 
              A better implementation would stream logs directly to s3 from memory,
              instead of uploading a file produced by another logging handler.
              However, this is simple and works.
        """
        super().__init__()

        self.bucket = bucket
        self.key = key
        self.filename = filename
        self.s3 = boto3.client("s3")

    def emit(self, record):
        """
            Upload to s3
            TODO: directly stream record to s3 instead of file
        """
        self.s3.upload_file(self.filename, self.bucket, self.key)

