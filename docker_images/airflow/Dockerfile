FROM apache/airflow:slim-2.6.2

RUN python -m pip install --upgrade pip
RUN python -m pip install apache-airflow-providers-apache-spark

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Disable workers refresh
ENV AIRFLOW__WEBSERVER__WORKER_REFRESH_BATCH_SIZE = 0
ENV AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL = 0

USER airflow

RUN python -m pip install --upgrade pip

COPY ./requirements.txt /
RUN pip install -r /requirements.txt
# RUN python -m pip install apache-airflow-providers-apache-spark
# RUN python -m pip install 'apache-airflow[amazon]'
# RUN python -m pip install apache-airflow-providers-amazon

# docker build -t dario248/airflow:slim_2_6_2 .