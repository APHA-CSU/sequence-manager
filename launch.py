import argparse
import sys
import subprocess

# install dependancies
def install():
    subprocess.run(["bash", "install.bash"], check=True)



def main(args):
    parser = argparse.ArgumentParser(description="Run a job for the TB reprocess")
    parser.add_argument("job", help="Job ID")

    parsed = parser.parse_args()

if __name__ == '__main__':
    main(sys.argv[1:])