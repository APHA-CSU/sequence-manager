import argparse
import sys
import subprocess
import os
import tempfile
import logging

import pandas as pd

from s3_logging_handler import S3LoggingHandler

# Launch and manage jobs for the TB reprocess

# TODO: set image to prod
DEFAULT_IMAGE = "aphacsubot/btb-seq:master"
DEFAULT_RESULTS_PREFIX_URI = "s3://s3-staging-area/nickpestell/results/"
DEFAULT_BATCHES_URI = "s3://s3-staging-area/nickpestell/batches.csv"
LOGGING_BUCKET = "s3-staging-area"
LOGGING_PREFIX = "nickpestell/logs/"

def launch(job_id, results_prefix_uri=DEFAULT_RESULTS_PREFIX_URI, batches_uri=DEFAULT_BATCHES_URI):
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
        reads_uri = f's3://{batch["bucket"]}/{batch["prefix"]}'
        results_uri = f'{results_prefix_uri}/{batch["prefix"]}/'
        
        try:
            run_pipeline_s3(reads_uri, results_uri)

        except Exception as e:
            logging.exception(e)

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
    subprocess.run([
        "sudo", "docker", "run", "--rm", "-it",
        "-v", f"{reads}:/reads/",
        "-v", f"{results}:/results/",
        image,
        "bash", "./btb-seq", "/reads/", "/results/",
    ], check=True)

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
