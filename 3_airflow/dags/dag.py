from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from pathlib import Path
from pendulum import datetime

profile_config_dev = ProfileConfig(
    profile_name="p_dw",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="docker_postgres_db",
        profile_args={"schema": "public"},
    ),
)

dbt_env = Variable.get("dbt_env", default_var="dev").lower()
if dbt_env not in ("dev", "prod"):
    raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

#profile_config = profile_config_dev if dbt_env == "dev" else profile_config_prod

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(Path("/usr/local/airflow/dbt/p_dw")),
    profile_config=profile_config_dev,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "target": profile_config_dev.target_name,
    },
    schedule="@daily",
    start_date=datetime(2025, 5, 30),
    catchup=False,
    dag_id=f"dag_jornada_dw_{dbt_env}"
    #atualizando
)