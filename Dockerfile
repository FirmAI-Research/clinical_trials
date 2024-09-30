
FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

# Set the command to run the pipeline by default
CMD ["python", "dlt_pipeline.py"]
