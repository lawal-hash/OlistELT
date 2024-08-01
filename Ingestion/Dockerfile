FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y python3 && apt-get install -y python3-pip && apt-get install zip unzip
RUN pip install kaggle
RUN mkdir /root/.kaggle
COPY *.json /root/.kaggle
RUN chmod 600 /root/.kaggle/kaggle.json
RUN mkdir /root/ingestion 
WORKDIR /root/ingestion
COPY . .
CMD ["bash", "src/download.sh"]
