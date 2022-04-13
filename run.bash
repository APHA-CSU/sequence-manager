sudo docker build -t tbbatch  .


sudo docker run --privileged --rm -it -v $PWD:/repo/ tbbatch /bin/bash

