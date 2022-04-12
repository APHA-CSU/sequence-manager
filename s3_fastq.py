import boto3
import pandas as pd
import subprocess
import os

import re

def list_keys(bucket_name, prefix):
    """ Returns a list of all keys matching a prefix in a S3 bucket """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    objects = bucket.objects.filter(Prefix=prefix)
    return [obj.key for obj in objects]

def pair_files(keys):
    """ Pair fastq read files and extract metadata from a list of keys. 
        Follows the CSU naming convention <prject_code>/
    
        Returns four dataframes: 
            samples - paired samples from the keys
            plates - summary of groupings of samples
            unpaired - keys that didn't form a sample pair
            not_parsed - keys that were unparsable

        Each key appears in one of the samples/unpaired/not_parsed dataframes exactly once.
    """
    # This pattern has been tested on regexr with a selection of test cases
    # TODO: unit test
    pattern = r'(.+)\/(?:(.+)_)?(\w+)\/([^_]+)(?:_S(\d+))?(?:.+)?_R(\d)_(\d+)\.fastq\.gz'

    # Sorting the keys puts paired files next (or close) to each other
    keys = sorted(keys)

    samples = []
    unpaired = []
    not_parsed = []

    # Loop over keys, detect named pairs, extract metadata
    while len(keys)>=2:
        # Select keys
        key_1 = keys.pop(0)
        key_2 = keys.pop(0)

        # Check is the keys match
        match_1 = re.findall(pattern, key_1)
        match_2 = re.findall(pattern, key_2)
        
        # Non-matching naming convention
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

        # The two keys do not form a correct match
        if not is_match:
            unpaired.append(key_1)
            keys.insert(0, key_2)
            continue        

        # Create Sample Object
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
        sample["plate_id"] = f'{sample["sequencer"]}_{sample["run_id"]}' 
        samples.append(sample)

    # Last key remaining
    unpaired.extend(keys)

    # All keys should exist once across these output dataframes
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
    """ Returns a dataframe that summarises the plates that feature in a samples dataframe as produced by pair_keys """
    # Group by plate_id
    df = samples.groupby("plate_id")

    # Plate metadata from an arbitrary sample.
    # TODO: Ensure these columns are consistent across each group
    summary = df.max().loc[:, ['sequencer', 'run_id', 'project_code']]

    # Additional plate columns: num_samples / plate_id / uri prefix
    plate_sizes = df.size().to_frame().rename(columns={0: "num_samples"})
    summary = summary.merge(plate_sizes, left_on="plate_id", right_on="plate_id")

    summary["prefix"] = df.apply(lambda x: os.path.dirname(x["read_1"].max()) + '/')

    # more convenient to have the plate_id as a column rather than the index
    summary = summary.reset_index(level=0)
    
    return summary

def bucket_summary(bucket, prefixes):
    """ summarises the samples in a bucket from a list of prefixes """
    keys = []
    for prefix in prefixes:
        keys.extend(list_keys(bucket, prefix))

    samples, plates, unpaired, not_parsed = pair_files(keys)

    samples["bucket"] = bucket
    plates["bucket"] = bucket
    unpaired["bucket"] = bucket
    not_parsed["bucket"] = bucket

    return samples, plates, unpaired, not_parsed

def list_tb_samples():
    """ summarises the TB samples. Produces a number of csvs locally """
    # Summarise
    samples_1, plates_1, unpaired_1, not_parsed_1 = bucket_summary('s3-csu-001', ['SB4030/', 'SB4030-TB/', 'SB4020/', 'SB4020-TB/'])
    samples_2, plates_2, unpaired_2, not_parsed_2 = bucket_summary('s3-csu-002', ['SB4020-TB/'])

    # Combine + csv output
    pd.concat([samples_1, samples_2], ignore_index=True).to_csv('samples.csv')
    pd.concat([plates_1, plates_2], ignore_index=True).to_csv('plates.csv')
    pd.concat([unpaired_1, unpaired_2], ignore_index=True).to_csv('unpaired.csv')
    pd.concat([not_parsed_1, not_parsed_2], ignore_index=True).to_csv('not_parsed.csv')

if __name__ == '__main__':
    df = list_tb_samples()
