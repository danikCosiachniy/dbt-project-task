import sys
from typing import Any

from loguru import logger

from airflow.models import TaskInstance

# Remove default Loguru handler to avoid duplicate logs
logger.remove()

# Console handler: human-readable logs to stderr
logger.add(
    sys.stderr,
    format='<green>{time:YYYY-MM-DD HH:mm:ss}</green> | '
    '<level>{level: <8}</level> | '
    '<cyan>{function}</cyan> - <level>{message}</level>',
    level='INFO',
)

# Path to the central dbt/Airflow log file inside the container
LOG_FILE_PATH: str = '/opt/airflow/logs/dbt_pipeline.log'

# File handler: structured JSON logs with rotation & compression
logger.add(
    LOG_FILE_PATH,
    rotation='10 MB',
    retention='10 days',
    compression='zip',
    level='DEBUG',
    serialize=True,
)


def log_success_callback(context: dict[str, Any]) -> None:
    """Logging success with loguru"""
    ti: TaskInstance = context.get('task_instance')

    logger.success(
        f'Task Success: {ti.task_id} '
        f'| DAG: {ti.dag_id} '
        f'| Duration: {ti.duration}s '
        f'| RunID: {ti.run_id}'
    )


def log_failure_callback(context: dict[str, Any]) -> None:
    """Logging error with loguru"""
    ti: TaskInstance = context.get('task_instance')
    exception = context.get('exception')

    logger.error(f'Task Failed: {ti.task_id} | DAG: {ti.dag_id} | Error: {exception}')


def log_start_callback(context: dict[str, Any]) -> None:
    """Info log of execution of the task with loguru"""
    ti: TaskInstance = context.get('task_instance')

    logger.info(f'Executing Task: {ti.task_id} | DAG: {ti.dag_id} | RunID: {ti.run_id}')
