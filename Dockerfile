FROM apache/airflow:2.5.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    vim \
  && apt-get install -y --reinstall gcc \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --upgrade pip
COPY requirements.txt $AIRFLOW_HOME
RUN pip install -r $AIRFLOW_HOME/requirements.txt

WORKDIR ${AIRFLOW_HOME}

# run "docker build . --tag airflow:2.5.1-python3.10" to construct the image
# "docker-compose up -d" to run containers