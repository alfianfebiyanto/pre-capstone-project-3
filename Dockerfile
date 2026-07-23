
FROM apache/airflow:2.9.1

COPY --chown=airflow:root requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt