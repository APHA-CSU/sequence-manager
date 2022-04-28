# sequence-manager

The sequence manager provides NGS software automation capabilities for the CUSP Lab, Weybridge and the CSU SCE3 community using python.

## Architecture

The tech stack for the processing of sequence data is shown below. Illumina machines in Weybridge generate raw `.bcl` sequencing data that is initially stored on `wey-001`, a server in the wet-lab. Following transfer of raw data, a file watcher running on `wey-001` (`bcl_manager`, see below) converts the `.bcl` data into `.fastq` files. The `.fastq` files are then transferred to a S3 bucket in the cloud.

![image](https://user-images.githubusercontent.com/6979169/124135441-b821fd80-da7b-11eb-8c64-eaed1084a8c6.png)

## `wey-001` Management

`wey-001` is a physical ubuntu production server that resides in the CUSP lab. Its primary responsibility is to accept raw data from the Illumina NGS machines, convert the raw bcl data to fastq and transfer data to the `s3-csu-001` bucket. This service is handled automatically by the `bcl_manager` (see below). To minimise the risks of data loss and service disruption, it's crucial the server is managed with care and caution. Think about your actions when interacting with the server, and ask questions if you are unsure. 

The device has two physical storage volumes:
- 1TB SSD mounted on `/`. Used to store the OS and incoming data from the Illumina machines that are connected via gigabit ethernet. The high bandwidth connection is required to protect against potential networking bottlenecks that could result in loss of data.
- 5TB RAID Storage mounted on `/Illumina/OutputFastq/`. Used to backup bcl data and fastq data. 

### Remote Access to `wey-001`

`wey-001` can be accessed remotely from a DEFRA computer using SSH. The `illuminashare` user is the common access user that runs production services. If you are not performing maintainance on the production service, you should login with your personal SCE username. To SSH into `wey-001`: 
1. SSH into the `ssh.int.sce.network` jumphost (see the [SCE SPOL article](https://defra.sharepoint.com/teams/Team741/SitePages/SSH-access-to-virtual-machine.aspx) for more information). This can be done through putty / cmd / an EC2 instance. 
2. SSH into `wey-001` from the jumphost: ```ssh username@wey-001```

### Data Management

WARNING: There is no automated mechanism to delete data from the machine. Storage levels need to be monitored and managed by the server administrator (Richard Ellis at the time of writing). Failure to manage storage levels can result in loss of data and disruptions to service. 

The key paths in `wey-001`:
- `/Illumina/IncomingRuns/` - Incoming data from the Illumina machines to the SSD
- `/Illumina/OutputFastq/BclRuns/` - Backup of the Bcl data onto the RAID storage
- `/Illumina/OutputFastq/FastqRuns/` - Fastq data converted from Bcl. The fastq files along with a `meta.json` file (see below) are automatically uploaded to S3. 

Runs are stored within directories with formatted names: `YYMMDD_instrumentID_runnumber_flowcellID`. The fastq files are automatically uploaded to S3 according to project code, along with a `meta.json` file that contains metadata associated with the intstrument's run. This file makes it easier to search/access metadata associated with each batch of samples, form databases, and write automation routines. The json file has format (see example above):
```
{
    "project_code": string,
    "instrument_id": string,
    "run_number": string,
    "run_id": string",
    "flowcell_id": string,
    "sequence_date": string,
    "upload_time": string
}
```

### Raw Bcl Example

An example of the raw data that is generated by Illumina machines is shown below. The `CopyComplete.txt` file is transferred last and triggers file-watching events on the `bcl_manager`.

![image](https://user-images.githubusercontent.com/6979169/165509245-9ee64350-6063-4e88-af6c-7906989e0577.png)


### Fastq Data and Upload to S3 Example

Example of converted fastq output stored under `/Illumina/OutputFastq/FastqRuns/` is shown below. The data is stoted in a directory with the same name as the originating bcl data. Fastq files are stored in subdirectories named by their project code. 

![image](https://user-images.githubusercontent.com/6979169/165512161-1212f51c-c9d1-4402-bac5-26cd08d17f86.png)

The example would generate a `meta.json` under `s3://s3-csu-001/SB4030/NB501786_0396/meta.json` that looks like:

```
{
    "project_code": "SB4030",
    "instrument_id": "NB501786",
    "run_number": "0396",
    "run_id": "NB501786_0396",
    "flowcell_id": "AHKGT5AFX3",
    "sequence_date": "2022-04-01",
    "upload_time": "2022-04-27 13:21:09.418487"
}
```

A similar `meta.json` would also be stored under `s3://s3-csu-001/FZ2000/NB501786_0396/meta.json`

## Bcl Management

`bcl_manager.py` is a file-watching service that runs on `wey-001` for automated: 
- Backup of raw .`bcl` data locally
- Conversion of raw `.bcl` data into `.fastq`
- Upload of `.fastq` files to S3 according to project code

For monitoring purposes, the manager logs events to `./bcl-manager.log` and S3 (default: `s3://s3-csu-001/aaron/logs/bcl-manager.log`)

### Installation

To run `bcl_manager.py`, python dependancies need to be installed:

```
cd /path/to/repo/
pip install -r requirements.txt
```

### Running the bcl manager in development

To run a development version of the `bcl_manager` service:
```
python bcl_manager.py
```

### Running the bcl manager in production

When updating / running the bcl manager in production, it is essential to protect against data loss by ensuring you aren't disrupting active runs by the Illumina machines. 

1. Inform the NGS lab manager (Saira Cawthraw at the time of writing) you will be performing maintainance on the server. Ensure there are no active runs on the Illumina machines that are likely to complete during the maintainance period. If completed runs are missed, they will have to be manually triggered once the `bcl_manager.py` service is running by re-creating the appropriate `CopyComplete.txt` file. 
2. SSH into the `wey-001` (see above)
3. [Screen](https://linuxize.com/post/how-to-use-linux-screen/) into the sessuion terminal using `screen -r`, or start a new one using `screen`. This ensures the service continues to run after the SSH session terminates
4. Perform any maintainance tasks, e.g. software updates
5. Ensure unit tests pass
```
python unit_tests.py
......
----------------------------------------------------------------------
Ran 6 tests in 0.022s

OK

```
6. Run the Bcl Manager
```
python bcl_manager.py
```
8. Detach from the session
```
Ctrl+a d
```

The SSH tunnel can then be terminated with the Bcl Manager still running. To return to the screen, SSH into `wey-001` and call:
```
screen -r
```

### Event Handling

By default, `bcl_manager.py` watches `/Illumina/IncomingRuns/` for incoming data. This path corresponds to a location on the SSD in `wey-001` where Illumina machines  store generated bcl data over gigabit ethernet. File watch events are triggered by the `CopyComplete.txt` file that's generated by the Illumina machines within the directory it creates for storing the run data (see below). The directory name is expected to be formatted: `yymmdd_instrumentID_runnumber_flowcellID`. 

The `bcl_manager.py` event handler makes a copy of the raw bcl data to the `backup-dir` (default: `/Illumina/OutputFastq/BclRuns/`). This default path corresponds to  a location on the high-storage RAID disk on `wey-001`. 

Following back-up, the bcl data is converterd to `fastq.gz` format using Illumina's [bcl2fastq](https://emea.support.illumina.com/sequencing/sequencing_software/bcl-convert.html) under the `fsatq-dir` (default: `/Illumina/OutputFastq/FastqRuns/`). 

The fastq data is then uploaded to S3 according to `s3://{bucket}/{prefix}/{project_code}/{run_id}/` (default: `s3://s3-csu-001/{project_id}/{run_number}/`). The `project_code` is inferred from the bcl directory structure (see below). The `run_id` is formatted as `instrumentid_runnumber` and is also inferred from the bcl directory structure. 


### Logs and Error Handling

The Bcl Manager is designed to exit if processing fails in any way. This could occur for a number of reasons:
1. The sample sheet for the Illumina run contains errors
2. Duplicate run id
3. Low space on `wey-001`

Error information is automatically logged and uploaded to `s3://s3-csu-003/aaron/logs/bcl-manager.log` in the event of processing failure. The available space on `wey-001` is also reported at the end of each run.

Once the error has been diagnosed and fixed by a maintainer, `bcl_manager.py` can be restarted as described above.

![image](https://user-images.githubusercontent.com/6979169/124142307-0803c300-da82-11eb-9902-a2404c526c36.png)

