FROM postgres:13

RUN apt update && \
    apt install -y gnupg postgresql-common apt-transport-https lsb-release wget && \
    /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh && \
    echo "deb https://packagecloud.io/timescale/timescaledb/debian/ $(lsb_release -c -s) main" | tee /etc/apt/sources.list.d/timescaledb.list && \
    wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add - && \
    apt update && \
    apt install -y timescaledb-2-postgresql-13 && \
    timescaledb-tune -quiet 