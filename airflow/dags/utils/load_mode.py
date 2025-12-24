from airflow.models import Variable
from utils.constants import FLAG_NAME


def get_flag() -> bool:
    """
    Determine whether a full refresh (initial-load) should be executed.

    The function reads the Airflow Variable defined by ``FLAG_NAME`` and treats the
    value ``"true"`` as "already initialized". If the flag is missing or not equal
    to ``"true"``, the function returns ``True`` to indicate that the DAG should run
    an initial-load (dbt full-refresh). If the flag is ``"true"``, it returns ``False``
    to indicate an incremental run.

    :return: True if the initialization flag is not set to "true" (run initial-load),
             otherwise False (run incremental-load).
    :rtype: bool
    """
    return Variable.get(FLAG_NAME, default_var='false').lower() != 'true'


def set_initialized_flag() -> None:
    """
    Mark the Data Vault as initialized.

    This function sets the Airflow Variable defined by ``FLAG_NAME`` to the string
    value ``"true"``. It is typically executed at the end of a successful initial-load
    run so that subsequent DAG runs switch to incremental mode.

    :return: None
    :rtype: None
    """
    Variable.set(FLAG_NAME, 'true')


def set_initialized_false() -> None:
    """
    Reset the Data Vault initialization flag.

    This function sets the Airflow Variable defined by ``FLAG_NAME`` to the string
    value ``"false"``. It is typically executed after a clean-up/reset procedure so
    that the next run of the main DAG performs an initial-load (dbt full-refresh).

    :return: None
    :rtype: None
    """
    Variable.set(FLAG_NAME, 'false')
