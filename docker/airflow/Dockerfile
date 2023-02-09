FROM apache/airflow:2.5.1

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && apt-get remove -y python \
  && apt-get install -y python3 r-base \
  && apt-get install -y openssl libssl-dev \
  && rm -rf /var/lib/apt/lists/*

RUN echo -e "AIRFLOW_UID=$(id -u)" > .env

RUN sudo usermod -a -G staff root
RUN R -e "install.packages(c('devtools', 'RPostgres', 'remotes'), dependencies=TRUE)"
RUN R -e "remotes::install_github('rfsaldanha/microdatasus')"

USER airflow

RUN python -m pip install --upgrade pip

COPY requirements.txt $AIRFLOW_HOME

RUN pip install -r $AIRFLOW_HOME/requirements.txt

WORKDIR ${AIRFLOW_HOME}

# run "docker build . --tag airflow:2.5.1" to construct the image
# "docker-compose up -d" to run containers