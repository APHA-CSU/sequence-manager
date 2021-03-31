# sequence-manager

The sequence manager provides NGS software automation capabilities in the lab and AWS using python.

## Installation

To install the necessary python dependancies

```
cd /path/to/repo/
pip install -r requirements.txt
```

## Bcl Manager

`bcl_manager.py` is used to automatically backup Bcl data transferred from Illumina machines onto `wey-002`. The Bcl Manager does three things
- Backup raw Bcl data to another directory
- Converts Bcl Data to Fastq
- Uploads Fastq to S3 according to project code

To run the Bcl Manager call:
```
python bcl_manager.py
```
