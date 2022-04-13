set -eo pipefail

# apt
sudo apt-get update
sudo apt-get -y install python3 python3-pip docker.io

# python 
sudo pip3 install biopython numpy pandas gitpython

