set -eo pipefail

#================================================================
# install.bash
#================================================================
#% DESCRIPTION
#%    Install dependancies for a job for the TB reprocess
#%
#% INPUTS
#%    job_id        id of the job

# apt
sudo apt-get update
sudo apt-get -y install python3 python3-pip docker.io systemctl

# TODO: install docker

# python 
sudo pip3 install biopython numpy pandas gitpython boto3

