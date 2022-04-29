# sequence-manager

The sequence manager provides NGS software automation capabilities for CUSP Lab, Weybridge and the CSU SCE3 community using python.

## Architecture

The tech stack for the processing of sequence data is shown below. Illumina machines in Weybridge generate raw `.bcl` sequencing data that is initially stored on `wey-001`, a server in the wet-lab. Following transfer of raw data, a file watcher running on `wey-001` (`bcl_manager`, see below) converts the `.bcl` data into `.fastq` files. The `.fastq` files are then transferred to a S3 bucket in the cloud.

![image](https://user-images.githubusercontent.com/6979169/124135441-b821fd80-da7b-11eb-8c64-eaed1084a8c6.png)


## Remote Access to `wey-001`

`wey-001` can be accessed remotely from a DEFRA computer: 
1. Open putty and ssh into the 10.233.2.44 port 22 jumphost (see the [SCE SPOL article](https://defra.sharepoint.com/teams/Team741/SitePages/SSH-access-to-virtual-machine.aspx) for more information)
2. SSH into `wey-001` from the jumphost: ```ssh illuminashare@wey-001```

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

To run the Bcl Manager, remote into `wey-001` and start the process in a new [screen session](https://linuxize.com/post/how-to-use-linux-screen/):
```
screen
python bcl_manager.py
```
Then detach from the screen using
```
Ctrl+a d
```

The putty instance can then be closed down with the Bcl Manager still running. To return to the screen, remote back into `wey-001` and call:
```
screen -r
```

### Maintainance and Error Handling

The Bcl Manager is designed to exit if processing fails in any way. This could occur for a number of reasons:
1. The sample sheet for the Illumina run contains errors
2. Duplicate run id
3. Low space on `wey-001`

Error information is automatically logged and uploaded to `s3://s3-csu-003/aaron/bcl_manager.log` in the event of processing failure. The available space on `wey-001` is also reported at the end of each run.

Once the error has been diagnosed and fixed by a maintainer, `bcl_manager.py` can be restarted as described above.

![image](https://user-images.githubusercontent.com/6979169/124142307-0803c300-da82-11eb-9902-a2404c526c36.png)


## TB Reprocessing

This repo also contains automation capabilities for reprocessing APHA's TB WGS Samples. The solution works by processing batches of samples in parallel across multiple EC2 instances. Each individual EC2 instance runs one batch at a time and log to their progress to a S3 bucket during processing. 

### Workflow

To reprocess the TB samples:

1. Run `summary.py` to generate a `batches.csv` file.
2. Plan how jobs will be delegated to machines by adding a `job_id` column to the `batches.csv` file
3. Configure the `launch.py` script
4. Launch jobs on EC2 machines

See below for details on how to perform each step. 

### (1) Summary of Raw TB Samples

The first step is to prepare a summary of the raw `fastq.gz` TB samples that are stored in the CSU `s3-csu-001` and `s3-csu-002` buckets:
```
python summary.py
```

This command produces three summary csv files:
- `samples.csv` - URI location of read pair files and associated metadata for each sequenced sample
- `batches.csv` - Batches of samples. Each run of the sequencer corresponds to a batch. Additional datasets manually curated by Richard are also included
- `not_parsed.csv` - Files in the buckets that do not conform to the sample naming convention. No `fastq.gz` should be included here. 
- `unpaired.csv` - Fastq files that do not have a read pair. Ideally this file should be empty.

Examine each of these files to gain an understanding of the data that exists, and perform any administration on errors that you may find. For example, you might find samples in `not_parsed.csv` that need to be renamed. Or you might find unpaired data in `unpaired.csv`.

### (2) Setting the `job_id`

Each individual EC2 machine runs a set batches one at a time. This step sets which batches run on which machines. 

Sets of batches are identified by the `job_id`. When a reprocessing job is launched on a machine with `launch.bash job_id`, a process downloads a `batches.csv` file from S3 and filters rows based on the `job_id`.

To prepare the `batches.csv` file:
- Append a column to the `batches.csv` file from step (1). I reccomend excel or libreoffice
- Fill out the rows in the `job_id` column. The id does not have to follow a specific format. e.g. "A", "3", "CSU-004" are all valid. 
- Upload the `batches.csv` to a location on S3 that is reachable by the job machines

### (3) Configuring the launch script

(a) Open the `launch.py` script and configure the global variables at the top
- `DEFAULT_IMAGE` - the docker image the job should run. Should be `prod` once the latest version of btb-seq is released. 
- `DEFAULT_PLATES_URI` - S3 URI that points to the `batches.csv` file uploaded in step (2) 
- `DEFAULT_ENDPOINT` - S3 URI prefix where results are stored
- `LOGGING_BUCKET` - S3 bucket that stores URIs
- `LOGGING_PREFIX` - S3 prefix that the log file is stored. 

(b) Commit the changes to a branch and commit to github

(c) To avoid github authentication setup during step (4), upload the code to a location on S3. For example:
```
aws s3 cp --recursive ./ s3://s3-csu-001/sequence-manager
```

### (4) Launch jobs on EC2 machines

For each job machine:
- SSH into the job machine via the SCE jumphost, `ssh.int.sce.network`
- copy the repo from S3
```
aws s3 cp --recursive s3://s3-csu-001/sequence-manager ./
```
- launch the job
```
sudo bash launch.bash job_id
```
