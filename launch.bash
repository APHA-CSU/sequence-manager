set -eo pipefail

#================================================================
# launch.bash
#================================================================
#% DESCRIPTION
#%    Install and launch a job for the TB reprocess
#%
#% INPUTS
#%    job_id        id of the job

JOB_ID=$1

bash install.bash
python3 launch.py $JOB_ID