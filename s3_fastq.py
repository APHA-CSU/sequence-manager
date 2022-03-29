import boto3
import pandas as pd
import subprocess

def boto():
    bucket_name = 's3-csu-001'
    project_codes = ['SB4030']

    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)

    project_code = project_codes[0]

    objects = bucket.objects.filter(Prefix=project_code + '/', Delimiter='/')

    summaries = []
    for obj in objects:
        object_type = "directory" if obj.key.endswith('/') else 'file'

        summary = {
            "key": obj.key,
            "type": object_type,
            "upload_date": str(obj.meta.__dict__['data']['LastModified'])
        }
        summaries.append(summary)

    df = pd.DataFrame(summaries)
    df.to_csv('./summary.csv')


def ls(uri):
    a = subprocess.run(["aws", "s3", "ls", uri], capture_output=True, check=True, text=True)
    lines = a.stdout.split('\n')

    files = []

    b = 2

def list_subfolder():
    bucket='s3-csu-001'
    prefix='SB4030'
    client = boto3.client('s3')
    result = client.list_objects(Bucket=bucket, Prefix=prefix+'/', Delimiter='/')

    return [o.get('Prefix') for o in result.get('CommonPrefixes')]

list_subfolder()
a=1