FROM  quay.io/astronomer/astro-runtime:13.0.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.9.0 && deactivate