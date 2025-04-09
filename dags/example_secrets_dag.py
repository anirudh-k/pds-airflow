"""Validate Airflow has secrets backend configured correctly."""

from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime, timedelta
from typing import Dict, Any


@task
def get_secrets() -> Dict[str, Any]:
    """Get secrets from Airflow."""
    conn = BaseHook.get_connection("gcp_standard")
    var = Variable.get("example_var")

    return {"connection": conn.extra, "variable": var}


with DAG(
    "example_secrets",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Example DAG showing how to use Airflow secrets",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 4, 8),
    catchup=False,
    tags=["example"],
) as dag:
    get_secrets()
