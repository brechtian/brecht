FROM postgres:9.4

USER root
RUN apt-get -y update
RUN apt install -y postgresql-common
RUN yes | sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh

USER postgres
ENV POSTGRES_PASSWORD=docker
ENV POSTGRES_USER=docker
ENV POSTGRES_DB=docker
RUN cat /usr/share/postgresql/postgresql.conf.sample > /etc/postgresql/postgresql.conf

RUN echo "wal_level=logical" >> /etc/postgresql/postgresql.conf
RUN echo "max_replication_slots=8" >> /etc/postgresql/postgresql.conf

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
