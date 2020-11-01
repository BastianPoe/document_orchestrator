FROM python:3.7-slim

COPY install-packages.sh /
RUN /install-packages.sh
  
COPY requirements.txt /
RUN pip install -r /requirements.txt

RUN apt-get -y remove --purge build-essential ; apt-get -y autoremove ; apt-get -y clean

COPY orchestrator.py /
WORKDIR /

CMD ["python3", "/orchestrator.py"]
