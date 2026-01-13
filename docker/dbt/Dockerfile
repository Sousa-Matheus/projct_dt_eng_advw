FROM python:3.11-slim

ENV DBT_HOME=/usr/app/dbt
WORKDIR $DBT_HOME

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
