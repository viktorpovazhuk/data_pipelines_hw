FROM apache/airflow:2.7.3

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

RUN mkdir /opt/airflow/credentials
COPY credentials/gcloud_cv_key.json /opt/airflow/credentials/gcloud_cv_key.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/credentials/gcloud_cv_key.json
ENV OPENAI_API_KEY=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