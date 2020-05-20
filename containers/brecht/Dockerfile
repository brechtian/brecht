FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y unzip curl wget supervisor software-properties-common lsb-release
RUN apt-get clean all
RUN apt-get install -y python-pip && pip install supervisor-stdout
RUN add-apt-repository ppa:rmescandon/yq && apt update && apt install yq -y

# Install Postgres

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" |tee  /etc/apt/sources.list.d/pgdg.list
RUN apt-get update && apt-get install -y postgresql-12 postgresql-client-12 postgresql-contrib-12
USER postgres
RUN    /etc/init.d/postgresql start &&\
    psql --command "CREATE USER docker WITH SUPERUSER PASSWORD 'docker';" &&\
    createdb -O docker docker
RUN echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/12/main/pg_hba.conf
RUN echo "listen_addresses='*'" >> /etc/postgresql/12/main/postgresql.conf
RUN echo "wal_level=logical" >> /etc/postgresql/12/main/postgresql.conf
RUN echo "max_replication_slots=8" >> /etc/postgresql/12/main/postgresql.conf
VOLUME  ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql"]

USER root

RUN mkdir -p /var/log/supervisor

# Install jabba
RUN wget https://github.com/shyiko/jabba/raw/master/install.sh
RUN chmod +x install.sh && ./install.sh
RUN . /root/.jabba/jabba.sh && jabba install openjdk@1.14.0 && jabba use openjdk@1.14.0
ENV PATH="/root/.jabba/jdk/openjdk@1.14.0/bin:${PATH}"

# Install kafka
RUN wget https://downloads.apache.org/kafka/2.5.0/kafka_2.13-2.5.0.tgz && tar -xvzf kafka_2.13-2.5.0.tgz
# Install prometheus
RUN wget https://github.com/prometheus/prometheus/releases/download/v2.18.1/prometheus-2.18.1.linux-amd64.tar.gz && tar -xvzf prometheus-2.18.1.linux-amd64.tar.gz
# Install grafana
RUN wget https://dl.grafana.com/oss/release/grafana-7.0.0.linux-amd64.tar.gz && tar -zxvf grafana-7.0.0.linux-amd64.tar.gz

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN yq w -i /prometheus-2.18.1.linux-amd64/prometheus.yml scrape_configs[0].static_configs[0].targets[0] localhost:9091

EXPOSE 5432
EXPOSE 9090
EXPOSE 3000
EXPOSE 8080
EXPOSE 9091
EXPOSE 9092

COPY core-0.1.zip core-0.1.zip
RUN unzip core-0.1.zip

CMD ["/usr/bin/supervisord"]

