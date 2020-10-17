FROM debian:sid-slim

RUN apt-get update ; apt-get -y dist-upgrade
RUN apt-get -y install python3 python3-pip
RUN pip3 install attachment-downloader

COPY orchestrator.py /
WORKDIR /

CMD ["python3", "/orchestrator.py"]
