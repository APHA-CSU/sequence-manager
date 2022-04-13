sudo docker build -t tbbatch  .


sudo docker run --rm -it -v $PWD:/repo/ tbbatch /bin/bash

