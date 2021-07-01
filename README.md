# sequence-manager

The sequence manager provides NGS software automation capabilities in the lab and AWS using python.

## Architecture

The tech stack for CUSP's processing of sequence data is shown below. Illumina machines in Weybridge generate raw `.bcl` sequencing data that is initially stored on `wey-001`, a server in the wet-lab. Following transfer of raw data, a file watcher running on `wey-001` (`bcl_manager`, see below) converts the `.bcl` data into `.fastq` files. The `.fastq` files are then transferred to a S3 bucket in the cloud.

![image](https://user-images.githubusercontent.com/6979169/124135441-b821fd80-da7b-11eb-8c64-eaed1084a8c6.png)


## Remote Access to `wey-001`

`wey-001` can be accessed remotely from a DEFRA computer: 
1. Open putty and ssh into the 10.233.2.44 port 22 jumphost (see the [SCE SPOL article](https://defra.sharepoint.com/teams/Team741/SitePages/SSH-access-to-virtual-machine.aspx) for more information)
2. SSH into `wey-001` from the jumphost ```illuminashare@wey-001```

## Installation

To install the necessary python dependancies

```
cd /path/to/repo/
pip install -r requirements.txt
```

## Bcl Manager Usage

`bcl_manager.py` is a file-watcher for automated: 
- Backup of raw .`bcl` data locally
- Conversion of raw `.bcl` data into `.fastq`
- Upload of `.fastq` files to S3 according to project code

### Running the manager

To run the Bcl Manager, SSH into `wey-001` and start the process in a new [screen session](https://linuxize.com/post/how-to-use-linux-screen/):
```
screen
python bcl_manager.py
```
Then detach from the screen using
```
Ctrl+a d
```

The putty instance can then be closed down with the Bcl Manager still running. To return to the screen, ssh back into `wey-001` and call:
```
screen -r
```

### Maintainance and Error Handling

The Bcl Manager is designed to exit if processing fails in any way. This could occur for a number of reasons:
1. The sample sheet for the Illumina run contains errors
2. Duplicate run id
3. Low space on `wey-001`

All errors are logged and uploaded to `s3://s3-csu-003/aaron/bcl_manager.log`. The log also contains 
