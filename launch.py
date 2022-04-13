import argparse
import sys
import subprocess

import pandas as pd

def launch(job_id):
    # Select plates
    plates = pd.read_csv('./plates.csv')
    plates = plates.loc[plates.job_id==job_id, :]

    print(plates)

def main(args):
    # Parse
    parser = argparse.ArgumentParser(description="Run a job for the TB reprocess")
    parser.add_argument("job", help="Job ID")

    parsed = parser.parse_args()

    # Run
    launch(parsed.job)

if __name__ == '__main__':
    main(sys.argv[1:])