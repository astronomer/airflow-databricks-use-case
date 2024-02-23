FROM quay.io/astronomer/astro-runtime:10.3.0

ENV AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES = airflow\.* astro\.* astro_databricks\.*