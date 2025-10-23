FROM apache/airflow:3.1.0
USER root
RUN apt-get update \
&& apt-get install -y --no-install-recommends \
        vim \
&& apt-get autoremove -yqq --purge \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt
