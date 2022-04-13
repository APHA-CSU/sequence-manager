import argparse
import sys
import subprocess

import pandas as pd

# TODO: set to prod
DEFAULT_IMAGE="aaronsfishman/bov-tb:master"

def launch(job_id):
    # Select plates
    plates = pd.read_csv('./plates.csv')
    plates = plates.loc[plates.job_id==job_id, :]

    for i, plate in plates.iterrows():
        uri = f's3://{plate["bucket"]}/{plate["prefix"]}'
        endpoint = 's3://s3-csu-001/test/'
        run_pipeline(uri, endpoint)        

    print(plates)

def run_pipeline(reads, results, image=DEFAULT_IMAGE):
    """ Run the pipeline using docker """
    subprocess.run(["sudo", "docker", "pull", image], check=True)

    return
    subprocess.run([
        "sudo", "docker", "run", "--rm", "-it",
        image,
        "/root/nextflow/nextflow", "SCE3_pipeline_update.nf",
    ], check=True)

def main(args):
    # Parse
    parser = argparse.ArgumentParser(description="Run a job for the TB reprocess")
    parser.add_argument("job", help="Job ID")

    parsed = parser.parse_args()

    # Run
    launch(parsed.job)

if __name__ == '__main__':
    main(sys.argv[1:])