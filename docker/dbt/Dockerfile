FROM python:3.11-slim

ENV DBT_HOME=/usr/app/dbt
WORKDIR $DBT_HOME

RUN apt-get update \
 && apt-get install -y \
    git \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
