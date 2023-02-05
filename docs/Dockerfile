FROM apache/airflow:2.5.1
COPY requirements.txt .
RUN apt-get update && apt-get install -y r-base r-base-core && \
    rm -r /var/lib/apt/lists/*
# RUN python3 -m pip install rpy2
RUN pip install -r requirements.txt