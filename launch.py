import argparse
import sys
import subprocess
from matplotlib.pyplot import subplot2grid
import os

import pandas as pd

# TODO: set to prod
DEFAULT_IMAGE = "aaronsfishman/bov-tb:master"
DEFAULT_ENDPOINT = "s3://s3-staging-area/AaronFishman/"

def launch(job_id, endpoint=DEFAULT_ENDPOINT):
    # Select plates
    plates = pd.read_csv('./plates.csv')
    plates = plates.loc[plates.job_id==job_id, :]

    for i, plate in plates.iterrows():
        reads_uri = f's3://{plate["bucket"]}/{plate["prefix"]}'
        results_uri = f'{endpoint}/{plate["prefix"]}/'
        
        run_pipeline_s3(reads_uri, results_uri)        


def run_pipeline_s3(reads_uri='s3://s3-csu-001/SB4020-TB/10195/', results_uri='s3://s3-staging-area/AaronFishman/docker-1/', image=DEFAULT_IMAGE):
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