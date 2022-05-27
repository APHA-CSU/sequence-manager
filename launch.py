import argparse
from asyncore import file_dispatcher
import sys
import subprocess
import os
import tempfile
import logging
import glob
import time
import re

import pandas as pd

from s3_logging_handler import S3LoggingHandler
import summary

# Launch and manage jobs for the TB reprocess

# TODO: set image to prod
DEFAULT_IMAGE = "aphacsubot/btb-seq:master"
DEFAULT_RESULTS_BUCKET = "s3-csu-003"
DEFAULT_RESULTS_S3_PATH = "v3"
DEFAULT_BATCHES_URI = "s3://s3-csu-001/config/batches.csv"
DEFAULT_SUMMARY_PREFIX = "v3/summary" 
LOGGING_BUCKET = "s3-csu-003"
LOGGING_PREFIX = "logs"
DEFAULT_SUMMARY_FILEPATH = os.path.join(os.getcwd(), "summary.csv")

def launch(job_id, results_bucket=DEFAULT_RESULTS_BUCKET, results_s3_path=DEFAULT_RESULTS_S3_PATH, 
           batches_uri=DEFAULT_BATCHES_URI, summary_prefix=DEFAULT_SUMMARY_PREFIX, 
           summary_filepath=DEFAULT_SUMMARY_FILEPATH):
    """ Launches a job for a specific EC2 instance """

    # Download batches csv from S3
    logging.info(f"Downloading batches csv from {batches_uri}")
    subprocess.run(["aws", "s3", "cp", batches_uri, "./batches.csv"])
    batches = pd.read_csv('./batches.csv')
    batches = batches.loc[batches.job_id==job_id, :].reset_index(level=0)

    # Process one plate at a time
    for i, batch in batches.iterrows():
        logging.info(f"""
            Running batch {i+1}/{len(batch)}
                bucket: {batch["bucket"]}
                prefix: {batch["prefix"]}
        """)
        reads_uri = os.path.join(f's3://{batch["bucket"]}', batch["prefix"])
        results_prefix = os.path.join(results_s3_path, batch["prefix"])
        results_uri = os.path.join(f's3://{results_bucket}', results_prefix)

        # temp directory ensures we don't get lots of data accumulating
        with tempfile.TemporaryDirectory() as temp_dirname:
            try:
                run_pipeline_s3(reads_uri, results_uri, temp_dirname)
                append_summary(batch, results_prefix, summary_filepath, temp_dirname)

            except Exception as e:
                logging.exception(e)
                raise e

    # Push summary csv file to s3
    summary_uri = os.path.join(f's3://{results_bucket}', summary_prefix, f'{job_id}.csv')
    try:
        subprocess.run(["aws", "s3", "cp", "--acl", "bucket-owner-full-control", summary_filepath, summary_uri], check=True)

    except Exception as e:
        logging.exception(e)
        raise e

def run_pipeline_s3(reads_uri, results_uri, work_dir, image=DEFAULT_IMAGE):
    """ Run pipeline from S3 uris """
    
    # Validate input
    if not reads_uri.startswith("s3://"):
        raise Exception(f"Invalid reads uri: {reads_uri}")

    if not results_uri.startswith("s3://"):
        raise Exception(f"Invalid results uri: {results_uri}")
    
    # Make I/O Directories
    temp_reads = f"{work_dir}/reads/"
    temp_results = f"{work_dir}/results/"

    os.makedirs(temp_reads)
    os.makedirs(temp_results)

    # Download
    subprocess.run(["aws", "s3", "cp", "--recursive", reads_uri, temp_reads], check=True)
    
    # Run
    run_pipeline(temp_reads, temp_results)
    
    # Upload
    subprocess.run(["aws", "s3", "cp", "--recursive", temp_results, results_uri], check=True)

def run_pipeline(reads, results, image=DEFAULT_IMAGE):
    """ Run the pipeline using docker """

    # docker requires absolute paths
    reads = os.path.abspath(reads)
    results = os.path.abspath(results)

    # pull and run
    subprocess.run(["sudo", "docker", "pull", image], check=True)
    ps = subprocess.run(["sudo", "docker", "run", "--rm", "-it",
                         "-v", f"{reads}:/reads/",
                         "-v", f"{results}:/results/",
                         image, "bash", "./btb-seq", "/reads/", "/results/",], 
                         check=True)

def append_summary(batch, results_prefix, summary_filepath, work_dir):
    """
        Appends to a summary csv file containing metadata for each sample including reads and results
        s3 URIs.
    """
    # get reads metadata
    df_reads, _, _, _ = summary.bucket_summary(batch["bucket"], [batch["prefix"]])
    # download metadata for the batch from AssignedWGSCluster csv file
    results_path = glob.glob(f'{work_dir}/results/Results*')
    results_path = results_path[0]
    assigned_wgs_cluster_path = glob.glob(f'{results_path}/*AssignedWGSCluster*.csv')
    df_results = pd.read_csv(assigned_wgs_cluster_path[0])
    # add columns for reads and results URIs
    df_results.insert(1, "results_bucket", "s3-csu-003")
    df_results.insert(2, "results_prefix", os.path.join(results_prefix, results_path.split(os.path.sep)[-1]))
    df_results.insert(3, "sequenced_datetime", time.strftime("%d-%m-%y %H:%M:%S"))
    # join reads and results dataframes
    df_joined = df_reads.join(df_results.set_index('Sample'), on='sample_name', how='outer')
    # if summary file already exists locally - append to existing file
    if os.path.exists(summary_filepath):
        df_summary = pd.read_csv(summary_filepath)
        df_summary = pd.concat([df_summary, df_joined]).reset_index(drop=True)
    # else create new
    else:
        df_summary = df_joined
    df_summary.to_csv(summary_filepath, index=False)

def main(args):
    # Parse
    parser = argparse.ArgumentParser(description="Run a job for the TB reprocess")
    parser.add_argument("job", help="Job ID")

    parsed = parser.parse_args(args)

    # Setup logging
    log_prefix = f"{LOGGING_PREFIX}/{parsed.job}"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            S3LoggingHandler(f'./{parsed.job}.log', LOGGING_BUCKET, log_prefix)
        ]
    )

    # Run
    launch(parsed.job)

if __name__ == '__main__':
    main(sys.argv[1:])
