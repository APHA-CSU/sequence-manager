import argparse
import sys
import subprocess
from matplotlib.pyplot import subplot2grid
import os

import pandas as pd

# TODO: set image to prod
DEFAULT_IMAGE = "aaronsfishman/bov-tb:master"

# TODO: update uris
DEFAULT_ENDPOINT = "s3://s3-staging-area/AaronFishman/"
DEFAULT_PLATES_URI = "s3://s3-staging-area/AaronFishman/plates.csv"

def launch(job_id, endpoint=DEFAULT_ENDPOINT, plates_uri=DEFAULT_PLATES_URI):
    """ Launches a job for a specific EC2 instance """

    # Select plates
    subprocess.run(["aws", "s3", "cp", plates_uri, "./plates.csv"])
    plates = pd.read_csv('./plates.csv')
    plates = plates.loc[plates.job_id==job_id, :]

    # Process one plate at a time
    for i, plate in plates.iterrows():
        reads_uri = f's3://{plate["bucket"]}/{plate["prefix"]}'
        results_uri = f'{endpoint}/{plate["prefix"]}/'
        
        run_pipeline_s3(reads_uri, results_uri)        


def run_pipeline_s3(reads_uri, results_uri, image=DEFAULT_IMAGE):
    """ Run pipeline from S3 uris """
    
    # Validate input
    if not reads_uri.startswith("s3://"):
        raise Exception(f"Invalid reads uri: {reads_uri}")

    if not results_uri.startswith("s3://"):
        raise Exception(f"Invalid results uri: {results_uri}")
    
    # Make temp folder
    # TODO: use python temp folder to ensure it is removed
    temp_reads = f"./{reads_uri[5:]}/reads/"
    temp_results = f"./{reads_uri[5:]}/results/"

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

    # Run
    launch(parsed.job)

if __name__ == '__main__':
    # main(sys.argv[1:])
    main(["test"])