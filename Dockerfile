FROM python:3.7-alpine

COPY orchestrator.py /
WORKDIR /

CMD ["python3", "/orchestrator.py"]
