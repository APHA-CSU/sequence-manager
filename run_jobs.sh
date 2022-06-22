#!/bin/bash

set -eo pipefail

databucket=$1
jobsheet=$2
resultsbucket=$3
resultsfolder=$4
jobsToRun=$5

screen

# Get batch/job sheet
mkdir reprocess
aws s3 cp "$databucket"/reprocess/"$jobsheet" reprocess/"$jobsheet"

# read jobsheet and process selected batches

batchesToRun=$(awk -F, -v subset=$jobsToRun '$9 == subset' reprocess/"$jobsheet")
echo "$batchesToRun" > batchesToRun.txt

# process each selected batch using nextflow
file=batchesToRun.txt

echo -e "\tNow processing batches of samples with btb-seq..."
echo -e "\tInstance will shutdown when complete"

while IFS=, read -r num batch_id sequencer run_id project_code num_samples prefix bucket job_id;
do
    echo "Running $bucket/$prefix" >> $HOSTNAME.log
    nextflow run APHA-CSU/btb-seq -r prod -plugins nf-amazon -with-docker aphacsubot/btb-seq \
        --reads="s3://$bucket/$prefix*_{S*_R1,S*_R2}_*.fastq.gz" \
        --outdir="$resultsbucket/$resultsfolder" >/dev/null
    
    # Capture confirmation of completion
    time=$(printf "%(%d-%m-%y_%H:%M)T")
    echo "Completed processing of $bucket/$prefix at $time" >> $HOSTNAME.log

done <"$file"

# store log
aws s3 cp $HOSTNAME.log $resultsbucket/$resultsfolder/logs/$HOSTNAME.log

# auto shutdown instance when complete
shutdown

# concatenate outputs
# should be done when all reprocessing (from all instances) is complete
