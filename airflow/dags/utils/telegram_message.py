import urllib.parse
import urllib.request
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

from loguru import logger

from airflow.models import Variable
from utils.constants import VAR_TG_NAME


def _get_telegram_config() -> tuple[str, str]:
    cfg = Variable.get(VAR_TG_NAME, deserialize_json=True)
    return cfg['bot_token'], str(cfg['chat_id'])


def send_telegram_message(text: str, parse_mode: str = 'HTML') -> None:
    bot_token, chat_id = _get_telegram_config()

    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'

    parsed = urlparse(url)
    if parsed.scheme != 'https':
        raise ValueError('Only HTTPS URLs are allowed for Telegram API')

    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': parse_mode,
        'disable_web_page_preview': True,
    }

    data = urllib.parse.urlencode(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')  # noqa: S310

    with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310
        _ = resp.read()


def build_message(
    context: dict[str, Any],
    status: str,
    emoji: str,
    extra: str | None = None,
) -> str:
    ti = context.get('task_instance')

    dag_id = getattr(ti, 'dag_id', 'unknown_dag')
    task_id = getattr(ti, 'task_id', 'unknown_task')
    run_id = context.get('run_id', 'unknown_run')
    duration = getattr(ti, 'duration', 'n/a')
    log_url = getattr(ti, 'log_url', '')

    msg = (
        f'{emoji} <b>{status}</b>\n'
        f'<b>DAG:</b> {dag_id}\n'
        f'<b>Task:</b> {task_id}\n'
        f'<b>Run:</b> {run_id}\n'
        f'<b>Duration:</b> {duration}s\n'
    )
    if log_url:
        msg += f'<b>Log:</b> {log_url}\n'
    if extra:
        msg += f'<b>Details:</b> {extra[:900]}\n'

    return msg


def safe_notify_telegram(
    context: dict[str, Any],
    status: str,
    emoji: str,
    extra: str | None = None,
) -> None:
    try:
        msg = build_message(context=context, status=status, emoji=emoji, extra=extra)
        send_telegram_message(msg)
    except (KeyError, ValueError, HTTPError, URLError) as exc:
        logger.warning('Telegram notification failed: {}', exc)
