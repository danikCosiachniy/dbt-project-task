import sys
from typing import Any

from loguru import logger

from airflow.models import TaskInstance
from utils.constants import LOG_FILE_PATH
from utils.telegram_message import safe_notify_telegram

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
    safe_notify_telegram(
        context=context, status='FAILED', emoji='❌', extra=str(exception) if exception else None
    )


def log_start_callback(context: dict[str, Any]) -> None:
    """Info log of execution of the task with loguru"""
    ti: TaskInstance = context.get('task_instance')

    logger.info(f'Executing Task: {ti.task_id} | DAG: {ti.dag_id} | RunID: {ti.run_id}')


def log_dag_success_callback(context: dict[str, Any]) -> None:
    """
    Log DAG success with loguru + send a single Telegram message
    when the whole DAG run succeeds.
    """
    dag = context.get('dag')
    dag_run = context.get('dag_run')

    dag_id = dag.dag_id if dag else (dag_run.dag_id if dag_run else 'unknown_dag')
    run_id = context.get('run_id', 'unknown_run')

    # Best-effort fields
    start_date = getattr(dag_run, 'start_date', None)
    end_date = getattr(dag_run, 'end_date', None)

    duration_s = None
    if start_date and end_date:
        duration_s = (end_date - start_date).total_seconds()

    state = getattr(dag_run, 'state', 'success')

    # Loguru logging
    logger.success(
        'DAG Success: dag_id={} | run_id={} | state={} | duration_s={} | start_date={} | end_date={}',
        dag_id,
        run_id,
        state,
        duration_s,
        start_date,
        end_date,
    )

    # Telegram message
    extra_parts = [
        f'State={state}',
    ]
    if duration_s is not None:
        extra_parts.append(f'Duration={int(duration_s)}s')
    if start_date:
        extra_parts.append(f'Start={start_date}')
    if end_date:
        extra_parts.append(f'End={end_date}')

    safe_notify_telegram(
        context=context,
        status='DAG SUCCEEDED',
        emoji='✅',
        extra=' | '.join(extra_parts),
    )
