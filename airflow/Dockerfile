FROM apache/airflow:2.9.0-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        procps \
        gdal-bin \
        libgdal-dev \
        libproj-dev \
        proj-data \
        proj-bin \
        libgeos-dev \
        libnetcdf-dev \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_HOME=/opt/airflow
ENV PATH=/home/airflow/.local/bin:${PATH}

# GDAL fix
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

WORKDIR ${AIRFLOW_HOME}

COPY requirements.txt .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

USER airflow

RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["/opt/airflow/entrypoint.sh"]