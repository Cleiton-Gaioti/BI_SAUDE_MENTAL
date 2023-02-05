FROM apache/airflow:2.5.1
USER root
COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && apt-get  install -y python3-pip \
    && apt-get install -y r-base \
    && R -e "install.packages(c('devtools', 'RPostgres', 'remotes'))" \
    && pip install -r requirements.txt \
    && apt-get install nano \
    && rm -rf /var/lib/apt/lists/*
USER airflow