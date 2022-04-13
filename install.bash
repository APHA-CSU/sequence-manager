set -eo pipefail

sudo apt-get update

sudo apt-get -y install python3 python3-pip docker.io

# python 
pip3 install biopython numpy pandas gitpython
sudo ln -s /usr/bin/python3 /usr/bin/python

