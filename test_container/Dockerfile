FROM ubuntu:24.04

COPY ./helpers/install_requirements.sh /
RUN /install_requirements.sh

ENV SPATIALITE_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu/mod_spatialite.so"

# Install MSSQL tools and drivers
RUN \
    # Download the package to configure the Microsoft repo \
    curl -sSL -O https://packages.microsoft.com/config/ubuntu/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)/packages-microsoft-prod.deb && \
    # Install the package \
    dpkg -i packages-microsoft-prod.deb && \
    # Delete the file \
    rm packages-microsoft-prod.deb && \
    # Install the driver \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    # optional: for bcp and sqlcmd \
    ACCEPT_EULA=Y apt-get install -y mssql-tools18 && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc && \
    # optional: for unixODBC development headers \
    apt-get install -y unixodbc-dev

ENV PATH="$PATH:/opt/mssql-tools18/bin"
ENV POSTGRES_HOST="postgres"
ENV POSTGRES_USER="gis"
ENV POSTGRES_PASSWORD="gis"
ENV POSTGRES_DB="gis"
ENV MYSQL_HOST="mysql"
ENV MYSQL_ROOT_PASSWORD="gis"
ENV MARIADB_HOST="mariadb"
ENV MARIADB_ROOT_PASSWORD="gis"
ENV MSSQL_HOST="geoalchemy2-mssql"
ENV MSSQL_SA_PASSWORD="GeoAlchemy2!Sql2025"
ENV MSSQL_TEST_DB="gis"
ENV MSSQL_TEST_LOGIN="gis"
ENV MSSQL_TEST_PASSWORD="gis"
ENV PYTEST_POSTGRESQL_DB_URL="postgresql://gis:gis@postgres/gis"
ENV PYTEST_MYSQL_DB_URL="mysql://gis:gis@mysql/gis"
ENV PYTEST_MARIADB_DB_URL="mariadb://gis:gis@mariadb/gis"
ENV PYTEST_MSSQL_DB_URL="mssql+pyodbc://gis:gis@geoalchemy2-mssql:1433/gis?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

COPY ./helpers/init_mssql.sh /
COPY ./helpers/entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
