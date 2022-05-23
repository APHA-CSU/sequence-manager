import argparse
from asyncore import file_dispatcher
import sys
import subprocess
import os
import tempfile
import logging
import glob

import pandas as pd

from s3_logging_handler import S3LoggingHandler

# Launch and manage jobs for the TB reprocess

# TODO: set image to prod
DEFAULT_IMAGE = "aphacsubot/btb-seq:master"
DEFAULT_RESULTS_BUCKET = "s3-staging-area"
DEFAULT_RESULTS_PREFIX = "nickpestell/v3"
DEFAULT_BATCHES_URI = "s3://s3-csu-001/config/batches.csv"
DEFAULT_SUMMARY_PREFIX = "nickpestell/v3/summary" 
DEFAULT_SUMMARY_FILEPATH = os.path.join(os.getcwd(), "summary.csv")
LOGGING_BUCKET = "s3-staging-area"
LOGGING_PREFIX = "logs"

def launch(job_id, results_bucket=DEFAULT_RESULTS_BUCKET, results_prefix=DEFAULT_RESULTS_PREFIX, 
           batches_uri=DEFAULT_BATCHES_URI, summary_prefix=DEFAULT_SUMMARY_PREFIX, 
           summary_filepath=DEFAULT_SUMMARY_FILEPATH):
    """ Launches a job for a specific EC2 instance """

    # Download batches csv from S3
    logging.info(f"Downloading batches csv from {batches_uri}")
    #subprocess.run(["aws", "s3", "cp", batches_uri, "./batches.csv"])
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
        results_prefix = os.path.join(results_prefix, batch["prefix"])
        results_uri = os.path.join(f's3://{results_bucket}', results_prefix)
        
        try:
            run_pipeline_s3(reads_uri, results_uri)
            append_summary(batch, results_uri, results_prefix, summary_filepath)

        except Exception as e:
            logging.exception(e)
            raise e

    # Push summary csv file to s3
    summary_uri = os.path.join(f's3://{results_bucket}', summary_prefix, job_id)
    try:
        subprocess.run(["aws", "s3", "cp", summary_filepath, summary_uri], check=True)

    except Exception as e:
        logging.exception(e)
        raise e

def run_pipeline_s3(reads_uri, results_uri, image=DEFAULT_IMAGE):
    """ Run pipeline from S3 uris """
    
    # Validate input
    if not reads_uri.startswith("s3://"):
        raise Exception(f"Invalid reads uri: {reads_uri}")

    if not results_uri.startswith("s3://"):
        raise Exception(f"Invalid results uri: {results_uri}")
    
    # Temp directory ensures we don't get lots of data accumulating
    with tempfile.TemporaryDirectory() as temp_dirname:
        # Make I/O Directories
        temp_reads = f"{temp_dirname}/reads/"
        temp_results = f"{temp_dirname}/results/"

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

def append_summary(batch, results_uri, results_prefix, summary_filepath):
    """
        Appends to a summary csv file containing metadata for each sample including reads and results
        s3 URIs.
    """
    # download metadata for the batch from AssignedWGSCluster csv file
    with tempfile.TemporaryDirectory() as temp_dirname:
        return_code = subprocess.run(['aws', 's3', 'sync',
            '--exclude', '*',
            '--include', '*AssignedWGSCluster*.csv',
            results_uri, temp_dirname
        ]).returncode
        if return_code:
            raise Exception( f"Error downloading AssignWGSCluster files: aws returned error code {return_code}")
        # read into pandas df
        dest_filepath = glob.glob(os.path.join(temp_dirname, "*", "*AssignedWGSCluster*.csv"))
        df = pd.read_csv(dest_filepath[0])
    # add columns for reads and results URIs
    df["reads_bucket"] = batch["bucket"]
    df["reads_prefix"] = batch["prefix"]
    df["results_bucket"] = "s3-csu-003"
    df["results_prefix"] = results_prefix
    # If summary file already exists locally - append to existing file
    if os.path.exists(summary_filepath):
        df_summary = pd.read_csv(summary_filepath)
        df_summary = pd.concat([df_summary, df], ignore_index=True)
    # else create new
    else:
        df_summary = df
    df_summary.to_csv(summary_filepath)

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
