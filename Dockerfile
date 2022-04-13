FROM ubuntu:20.04

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install sudo awscli

RUN useradd --home /home/default default 
RUN mkdir /home/default
RUN passwd -d default
RUN usermod -aG sudo default

USER default

WORKDIR /repo/