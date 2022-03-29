import glob
import re
import os
import subprocess

import pandas as pd

def read_assigned_wgs_cluster(dest_dir):
    """ read_assigned_wgs_cluster
    Combines *AssignedWGSCluster*.csv as downloaded from S3-CSU-003 buckets
    Positional argments:
        dest_dir- (string) path to directory for where to save data to
    Returns:
        combined WGS cluster csv as a DataFrame
    """

    # Locate filepaths. The glob pattern reflects the structure of s3-csu-003
    csv_filepaths = glob.glob(f'{dest_dir}/*/*/*AssignedWGSCluster*.csv')

    # Load each csv
    dfs = []
    for csv_filepath in csv_filepaths:
        df = pd.read_csv(csv_filepath)
        df['path'] = os.path.dirname(csv_filepath)
        dfs.append(df)

    # Combine into a single data frame
    df = pd.concat(dfs, axis=0, ignore_index=True)

    # Calculate additional columns for the wgs table
    samples_zero_padded = []
    consensus_paths = []
    has_consensus_paths = []
    for i,row in df.iterrows():
        print(f"{i} / {len(df)}")

        sample_name = str(row["Sample"])

        # Default value
        sample_zero_padded = 'UNABLE TO PARSE SAMPLE'
        consensus_path = "UNABLE TO FIND CONSENSUS PATH"
        has_consensus_path = True

        # Zero Pad sample name
        try:
            sample_zero_padded = zero_padded_sample_name(sample_name)
        except Exception as e:
            sample_zero_padded = str(e)

        # Find consensus path
        try:
            consensus_path = find_consensus_path(row)
        except Exception as e:
            has_consensus_path = False
            consensus_path = str(e)

        # Append to lists
        samples_zero_padded.append(sample_zero_padded)
        consensus_paths.append(consensus_path)
        has_consensus_paths.append(has_consensus_path)

    # Add new columns to df
    df['sample_zero_padded'] = samples_zero_padded
    df['consensus_path'] = consensus_paths
    df['has_consensus_path'] = has_consensus_paths

    return df

def zero_padded_sample_name(sample_name):
    """ zero_padded_sample_name
    Adds 5-level zero padding to AF numbers
    TODO: delete when sample names are consistent
    Positional argments:
        sample_name- (string) sample_name for where to save data to
    Returns:
        zero padded sample name
    """

    # Calculate zero padding
    # TODO: move me
    pattern = r'AF(?:\w)?-(\d+)-(\d+)-(\d+)'

    match = re.search(pattern, sample_name)
   
    if not match:
        raise Exception('UNABLE TO PARSE SAMPLE: '+ str(sample_name))

    return 'AF-%d-%05d-%d'%(int(match.group(1)), int(match.group(2)), int(match.group(3)))    

def find_consensus_path(row):
    """ find_consensus_path
    Finds path to consensus data given a row WGS data
    TODO: inputs of sample and path rather than row
    Positional argments:
        row- (Series) row of a WGS metadata DataFrame
    Returns:
        path to consensus file
    """

    sample = row['Sample']
    path = row['path']

    # Select consensus path
    consensus_paths = glob.glob(f"{path}/consensus/*.fas") +\
        glob.glob(f"{path}/*.fas")        

    matches = list(filter(lambda p: sample in p, consensus_paths))

    if not len(matches):
        raise Exception("MISSING: can\'t find consensus filepath for sample")
       
    if len(matches) > 1:
        raise Exception('Multiple consensus paths for this sample')

    return matches[0]

def s3_sync(s3_uris, dest_dir):
    """ s3_sync
    Download WGS metadata and consensus files from S3
    Positional argments:
        s3_uris- (list) S3 URIs
        dest_dir- (string) directory of where to store data to
    """

    # Download AssignWGSCluster csvs from S3
    include_glob_pattern = '*/consensus/*.fas' 
    for s3_uri in s3_uris:
        return_code = subprocess.run(['aws', 's3', 'sync',
            '--exclude', '*',
            '--include', '*AssignedWGSCluster*.csv',
            '--include', include_glob_pattern,
            s3_uri,  dest_dir
        ]).returncode

        if return_code:
            raise Exception( f"Error downloading AssignWGSCluster files: aws returned error code {return_code}")


if __name__ == '__main__':
    dest_dir = '/mnt/fsx-017/ViewBovine/'
    wgs_df = read_assigned_wgs_cluster(dest_dir)
    quit()
    path = '/mnt/fsx-017/ViewBovine/SB4020-TB/Results_10018_19Feb21'
    sample_name = 'AF-61-1225-15'

    load_consensus_metadata(wgs_df, 'sample_report_path.csv', dest_dir)