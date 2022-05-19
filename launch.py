import argparse
import sys
import subprocess
import os
import logging

import pandas as pd

from s3_logging_handler import S3LoggingHandler

# Launch and manage jobs for the TB reprocess

# TODO: set image to prod
DEFAULT_IMAGE = "aphacsubot/btb-seq:master"
DEFAULT_RESULTS_PREFIX_URI = "s3://s3-csu-003/v3/"
DEFAULT_BATCHES_URI = "s3://s3-csu-001/config/batches.csv"
LOGGING_BUCKET = "s3-csu-001"
LOGGING_PREFIX = "logs"

def launch(job_id, results_prefix_uri=DEFAULT_RESULTS_PREFIX_URI, batches_uri=DEFAULT_BATCHES_URI):
    """ Launches a job for a specific EC2 instance """

    # Download batches csv from S3
    logging.info(f"Downloading batches csv from {batches_uri}")
    #subprocess.run(["aws", "s3", "cp", batches_uri, "./batches.csv"])
    batches_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'batches.csv')
    batches = pd.read_csv(batches_path)
    job_id = 1
    batches = batches.loc[batches.job_id==job_id, :].reset_index(level=0)

    # Process one plate at a time
    for i, batch in batches.iterrows():
        logging.info(f"""
            Running batch {i+1}/{len(batch)}
                bucket: {batch["bucket"]}
                prefix: {batch["prefix"]}
        """)
        reads_uri = os.path.join(f's3://{batch["bucket"]}',batch["prefix"])
        results_uri = os.path.join(results_prefix_uri, batch["prefix"])
        
        try:
            run_pipeline(reads_uri, results_uri)

        except Exception as e:
            # NOTE: this is failing quietly, do we want this? I wonder if the exception should
            # be raised so that the run stops and the last info in the logging file is the exception
            # this will make it easier to manage missing output.
            logging.exception(e)

def run_pipeline(reads_uri, results_uri, image=DEFAULT_IMAGE):
    """ Run the pipeline using docker """

    # Validate input
    if not reads_uri.startswith("s3://"):
        raise Exception(f"Invalid reads uri: {reads_uri}")

    if not results_uri.startswith("s3://"):
        raise Exception(f"Invalid results uri: {results_uri}")

    # run
    try:
        branch = image.split(":")[1]
        ps = subprocess.run(f"nextflow run APHA-CSU/btb-seq -with-docker {image} -r {branch} "
                       f"--reads='{reads_uri}*_{{S*_R1,S*_R2}}*.fastq.gz' --outdir='{results_uri}'", 
                       shell=True, check=True, capture_output=True) 
        print(ps.stdout.decode())

    except subprocess.CalledProcessError as process_error:
        raise Exception(f"\nfailed process: \n\t'{process_error.cmd}' \n\nexit code: \n\t'{process_error.returncode}' \n\n"
              f"output: \n\t'{process_error.stdout.decode()}'\n\n")

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
