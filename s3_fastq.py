import boto3
import pandas as pd
import subprocess
import os

import re


def list_keys(bucket_name='s3-csu-001', prefix='SB4030/M02410_5267/'):
    s3 = boto3.resource('s3')

    bucket = s3.Bucket(bucket_name)

    objects = bucket.objects.filter(Prefix=prefix)

    return [obj.key for obj in objects]

def pair_files(keys):
    """
        Pair fastq read files
    """

    keys = sorted(keys)
    pattern = r'(.+)\/(?:(.+)_)?(\w+)\/([^_]+)(?:_S(\d+))?(?:.+)?_R(\d)_(\d+)\.fastq\.gz'

    samples = []
    unpaired = []
    not_parsed = []

    # Loop over each key and try to pair with the next file
    j = 0
    while len(keys)>=2:
        j+=1
        key_1 = keys.pop(0)
        key_2 = keys.pop(0)

        break_me = 'SB4020-TB/00522/A20U004247_S999_R1_001.fastq.gz'
        if (break_me in key_1) or (break_me in key_2):
            a = 1
            pass

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

    samples = pd.DataFrame(samples)
    unpaired = pd.DataFrame(unpaired, columns=['unpaired'])
    not_parsed = pd.DataFrame(not_parsed, columns=['not_parsed'])

    plates = plate_summary(samples)

    return (samples,
        plates,
        unpaired,
        not_parsed        
    )

def plate_summary(samples):
    df = samples.groupby("id")

    summary = df.max().loc[:, ['sequencer', 'run_id', 'project_code']]

    plate_sizes = df.size().to_frame().rename(columns={0: "num_samples"})

    summary = summary.merge(plate_sizes, left_on="id", right_on="id")

    summary["prefix"] = df.apply(lambda x: os.path.dirname(x["read_1"].max()) + '/')
    
    return summary

def list_tb_samples():
    # S3-CSU-001
    bucket='s3-csu-001'
    prefixes=['SB4030/', 'SB4030-TB/', 'SB4020/', 'SB4020-TB/']
    
    keys = []
    for prefix in prefixes:
        keys.extend(list_keys(bucket, prefix))

    pd.DataFrame(data=keys).to_csv(f'{bucket}_keys.csv')

    samples, plates, unpaired, not_parsed = pair_files(keys)

    samples.to_csv(f'./{bucket}_samples.csv')
    plates.to_csv(f'./{bucket}_plates.csv')
    unpaired.to_csv(f'./{bucket}_unpaired.csv')
    not_parsed.to_csv(f'./{bucket}_not_parsed.csv')

    # S3-CSU-002
    bucket='s3-csu-002'
    prefixes=['SB4020-TB/']
    
    keys = []
    for prefix in prefixes:
        keys.extend(list_keys(bucket, prefix))

    pd.DataFrame(data=keys).to_csv(f'{bucket}_keys.csv')

    samples, plates, unpaired, not_parsed = pair_files(keys)

    samples.to_csv(f'./{bucket}_samples.csv')
    plates.to_csv(f'./{bucket}_plates.csv')
    unpaired.to_csv(f'./{bucket}_unpaired.csv')
    not_parsed.to_csv(f'./{bucket}_not_parsed.csv')

    a = 1


keys = pd.read_csv('s3-csu-001_keys.csv')["0"].to_list()
a,b,c,d = pair_files(keys)
# b = 1


df = list_tb_samples()
print(df)
# a = df
