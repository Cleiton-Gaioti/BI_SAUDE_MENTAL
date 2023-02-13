FROM apache/airflow:2.5.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         r-base \
         openssl \
         libssl-dev \
         libcurl4-openssl-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN echo -e "AIRFLOW_UID=$(id -u)" > .env

RUN sudo usermod -a -G staff root
RUN sudo chmod 777 -R /usr/local/lib/R/site-library
RUN R -e "install.packages(c('devtools', 'RPostgres', 'remotes', 'RCurl', 'swirl'), dependencies=TRUE)"
RUN R -e "remotes::install_github('rfsaldanha/microdatasus')"

USER airflow

RUN pip install --upgrade pip
COPY requirements.txt $AIRFLOW_HOME
RUN pip install -r $AIRFLOW_HOME/requirements.txt

WORKDIR ${AIRFLOW_HOME}

# run "docker build . --tag airflow:2.5.1" to construct the image
# "docker-compose up -d" to run containers