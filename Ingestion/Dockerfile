FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y python3 && apt-get install -y python3-pip
RUN pip install kaggle
RUN mkdir /root/.kaggle
RUN mkdir root/ingestion 
WORKDIR root/ingestion
COPY . .
