FROM ubuntu:22.04

COPY ./helpers/install_requirements.sh /
RUN /install_requirements.sh

COPY ./helpers/init_postgres.sh /
env PGDATA="/var/lib/postgresql/data"
env POSTGRES_PATH="/usr/lib/postgresql/14"
RUN su postgres -c /init_postgres.sh

ENV SPATIALITE_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu/mod_spatialite.so"

COPY ./helpers/init_mysql.sh /
RUN /init_mysql.sh

COPY ./helpers/entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
