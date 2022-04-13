set -eo pipefail

JOB_ID=$1

bash install.bash

python3 launch.py $1