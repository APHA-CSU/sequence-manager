import boto3
import pandas as pd
import subprocess

import re

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

def list_keys(bucket_name='s3-csu-001', prefix='SB4030/M02410_5267/'):
    s3 = boto3.resource('s3')

    bucket = s3.Bucket(bucket_name)

    objects = bucket.objects.filter(Prefix=prefix)

    for obj in objects:
        summaries = []
        for obj in objects:
            object_type = "directory" if obj.key.endswith('/') else 'file'

            summary = {
                "key": obj.key,
                "type": object_type,
                "upload_date": str(obj.meta.__dict__['data']['LastModified'])
            }
            summaries.append(summary)

    return pd.DataFrame(summaries)

def pair_files():
    df = pd.read_csv('./files.csv')

    pattern = r'(\w+)\/(?:(.+)_)?(\w+)\/(.+)_(\w+)_R(\d)_(\d+)\.fastq\.gz'

    keys = sorted(list(df["key"]))

    if len(keys) % 2:
        raise Exception("Cannot pair files, uneven number of files")

    samples = []

    for i in range(0, len(keys), 2):
        key_1 = keys[i]
        key_2 = keys[i+1]

        match_1 = re.findall(pattern, key_1)
        match_2 = re.findall(pattern, key_2)

        # Ensure a match was made
        if not match_1 or len(match_1)!=1:
            raise Exception("Cannot parse: ", match_1)

        if not match_2 or len(match_2)!=1:
            raise Exception("Cannot parse: ", match_2)

        # Extract match
        match_1 = match_1[0]
        match_2 = match_2[0]

        # Check all fields match except the read pair
        for i, x in enumerate(match_1):
            if i == 5:
                continue

            if match_1[i]!=match_2[i]:
                raise Exception(f"Reads do not pair: {key_1} \n {key_2}")

        # Create Object
        sample = {
            "project_code": match_1[0],
            "sequencer": match_1[1],
            "run_id": match_1[2],
            "name": match_1[3],
            "S": match_1[4],
            "read_1": key_1,
            "read_2": key_2,
            "last": match_1[6]
        }
        samples.append(sample)


    return pd.DataFrame(samples)



def list_subfolders():
    bucket='s3-csu-001'
    prefix='SB4030'
    client = boto3.client('s3')
    result = client.list_objects(Bucket=bucket, Prefix=prefix+'/', Delimiter='/')

    return [o.get('Prefix') for o in result.get('CommonPrefixes')]


df = pair_files()
a = 1
# list_subfolder()
# pair_files()