import os
import re

import boto3
import pandas as pd

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
            batches - summary of groupings of samples
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

        # Check if the keys match
        match_1 = re.findall(pattern, key_1)
        match_2 = re.findall(pattern, key_2)
        
        # Non-matching naming convention / multiple matches
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
            "sequencer": match_1[1] if match_1[1] else "UnknownSequencer",
            "run_id": match_1[2],
            "name": match_1[3],
            "submission": extract_submission_no(match_1[3]),
            "well": match_1[4],
            "read_1": key_1,
            "read_2": key_2,
            "lane": match_1[6]
        }

        sample["batch_id"] = f'{sample["sequencer"]}_{sample["run_id"]}' 
        samples.append(sample)

    # Last key remaining
    unpaired.extend(keys)

    # All keys should exist once across these output dataframes
    samples = pd.DataFrame(samples)
    unpaired = pd.DataFrame(unpaired, columns=['unpaired'])
    not_parsed = pd.DataFrame(not_parsed, columns=['not_parsed'])

    batches = batch_summary(samples)

    return (samples,
        batches,
        unpaired,
        not_parsed        
    )

def batch_summary(samples):
    """ Returns a dataframe that summarises the batches that feature in a samples dataframe as produced by pair_keys """
    # Group by batch_id
    df = samples.groupby("batch_id")

    # Batch metadata from an arbitrary sample.
    # TODO: Ensure these columns are consistent across each group
    summary = df.max().loc[:, ['sequencer', 'run_id', 'project_code']]

    # Additional batch columns: num_samples / batch_id / uri prefix
    batch_sizes = df.size().to_frame().rename(columns={0: "num_samples"})
    summary = summary.merge(batch_sizes, left_on="batch_id", right_on="batch_id")

    summary["prefix"] = df.apply(lambda x: os.path.dirname(x["read_1"].max()) + '/')

    # more convenient to have the batch_id as a column rather than the index
    summary = summary.reset_index(level=0)
    
    return summary

def bucket_summary(bucket, prefixes):
    """ summarises the samples in a bucket from a list of prefixes """
    # Get list of keys from s3
    keys = []
    for prefix in prefixes:
        keys.extend(list_keys(bucket, prefix))

    # Parse
    samples, batches, unpaired, not_parsed = pair_files(keys)

    # Include bucket name
    samples["bucket"] = bucket
    batches["bucket"] = bucket
    unpaired["bucket"] = bucket
    not_parsed["bucket"] = bucket

    return samples, batches, unpaired, not_parsed

def extract_submission_no(sample_name):
    """ Extracts submision number from sample name using regex """
    pattern = r'\d{2,2}-\d{4,5}-\d{2,2}'
    matches = re.findall(pattern, sample_name)
    submission_no = matches[0] if matches else sample_name
    return submission_no

def main():
    """ summarises the TB samples. Produces a number of csvs locally """
    # Summarise
    samples_1, batches_1, unpaired_1, not_parsed_1 = bucket_summary('s3-csu-001', ['SB4030/', 'SB4030-TB/', 'SB4020/', 'SB4020-TB/'])
    samples_2, batches_2, unpaired_2, not_parsed_2 = bucket_summary('s3-csu-002', ['SB4020-TB/'])

    # Combine + csv output
    pd.concat([samples_1, samples_2], ignore_index=True).to_csv('samples.csv')
    pd.concat([batches_1, batches_2], ignore_index=True).to_csv('batches.csv')
    pd.concat([unpaired_1, unpaired_2], ignore_index=True).to_csv('unpaired.csv')
    pd.concat([not_parsed_1, not_parsed_2], ignore_index=True).to_csv('not_parsed.csv')

if __name__ == '__main__':
    main()
