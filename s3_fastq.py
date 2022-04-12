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

    return [obj.key for obj in objects]

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

def pair_files(keys):
    """
        Pair fastq read files
    """

    keys = sorted(keys)
    pattern = r'(.+)\/(?:(.+)_)?(\w+)\/([^_]+)(?:_S(\d+))?(?:.+)?_R(\d)_(\d+)\.fastq\.gz'

    samples = []
    unpaired = []
    not_parsed = []

    i = 0

    # Loop over each key and try to pair with the next file
    while len(keys)>=2:
        key_1 = keys.pop(0)
        key_2 = keys.pop(0)

        match_1 = re.findall(pattern, key_1)
        match_2 = re.findall(pattern, key_2)

        # Ensure a match was made
        if not match_1 or len(match_1)!=1:
            not_parsed.append(key_1)
            keys.insert(0, key_2)
            continue

        if not match_2 or len(match_2)!=1:
            not_parsed.append(key_2)
            keys.insert(0, key_1)
            continue

        # Extract match
        match_1 = match_1[0]
        match_2 = match_2[0]

        # Check all fields match except the read pair
        is_match = True
        for i, x in enumerate(match_1):
            if i == 5:
                if match_1[i] != '1' or match_2[i] != '2':
                    is_match = False
                    break
                continue

            if match_1[i] != match_2[i]:
                is_match = False
                break
                # raise Exception(f"Reads do not pair: {key_1} \n {key_2}")

        # The two keys do not form a correct match
        if not is_match:
            unpaired.append(key_1)
            keys.insert(0, key_2)
            continue        

        # Create Object
        sample = {
            "project_code": match_1[0],
            "sequencer": match_1[1],
            "run_id": match_1[2],
            "name": match_1[3],
            "well": match_1[4],
            "read_1": key_1,
            "read_2": key_2,
            "lane": match_1[6]
        }
        sample["id"] = f'{sample["sequencer"]}_{sample["run_id"]}' 
        samples.append(sample)

    unpaired.extend(keys)

    return pd.DataFrame(samples), pd.DataFrame(unpaired, columns=['unpaired']), pd.DataFrame(not_parsed, columns=['not_parsed'])

def list_subfolders(bucket='s3-csu-001', prefix='SB4030'):    
    client = boto3.client('s3')
    result = client.list_objects(Bucket=bucket, Prefix=prefix+'/', Delimiter='/')

    return [o.get('Prefix') for o in result.get('CommonPrefixes')]

def plate_summary(samples):
    df = samples.groupby("id")

    summary = df.max().loc[:, ['sequencer', 'run_id', 'project_code']]

    plate_sizes = df.size().to_frame().rename(columns={0: "num_samples"})

    summary = summary.merge(plate_sizes, left_on="id", right_on="id")
    
    return summary

def list_tb_samples(bucket='s3-csu-001', 
    prefixes=['SB4030', 'SB4030-TB', 'SB4020', 'SB4020-TB']
):
    # keys = []
    # for bucket, prefix in prefixes:
    #     keys.extend(list_keys(bucket, prefix))

    # df = keys.to_list()

    # quit()

    keys = pd.read_csv('./keys.csv')["0"].to_list()

    samples, unpaired, not_parsed = pair_files(keys)

    plates = plate_summary(samples)

    # samples.to_csv('./samples.csv')
    # unpaired.to_csv('./unpaired.csv')
    # not_parsed.to_csv('./not_parsed.csv')

    a = 1

df = list_tb_samples()
print(df)
# a = df
