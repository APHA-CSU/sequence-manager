set -eo pipefail

#================================================================
# install.bash
#================================================================
#% DESCRIPTION
#%    Install dependancies for a job for the TB reprocess

# apt
sudo apt-get update
sudo apt-get -y install python3 \
			python3-pip \
    			ca-certificates \
	        	curl \
		    	gnupg \
		        lsb-release


# docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
	    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get -y install docker-ce \
			docker-ce-cli \
			containerd.io \
			docker-compose-plugin

# python 
pip3 install biopython numpy pandas gitpython boto3
pip3 install --upgrade awscli

