# 1. Base Image
FROM apache/airflow:2.10.3

ENV AIRFLOW_HOME=/opt/airflow

# Switch to airflow user
USER airflow

# 2. Install Python Dependencies
# We are already the 'airflow' user by default in this image,
# so we can just run pip install directly.
RUN pip install --no-cache-dir \
    numpy \
    pandas \
    pyarrow \
    fastparquet \
    openpyxl \
    psycopg2-binary \
    sqlalchemy \
    lxml \
    html5lib \
    beautifulsoup4