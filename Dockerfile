FROM python:3.7-slim
  
COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY orchestrator.py /
WORKDIR /

CMD ["python3", "/orchestrator.py"]
