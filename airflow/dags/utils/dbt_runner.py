import os
import subprocess
import sys

from get_creeds import get_env

from utils.constants import DBT_PROJECT_PATH


def main():
    # Get arguments passed from Makefile
    dbt_args = sys.argv[1:]
    if not dbt_args:
        sys.exit(1)

    # Fetch credentials and prepare environment variables
    secrets = get_env()

    dbt_env = os.environ.copy()
    dbt_env.update(secrets)

    # Add user local bin to PATH to find dbt executable
    user_base = os.path.expanduser('~')
    local_bin = os.path.join(user_base, '.local/bin')

    current_path = dbt_env.get('PATH', '')
    if local_bin not in current_path:
        dbt_env['PATH'] = f'{local_bin}:{current_path}'

    # Construct the command
    cmd = ['dbt', *dbt_args]

    try:
        # Execute dbt command
        subprocess.run(cmd, cwd=DBT_PROJECT_PATH, env=dbt_env, check=True)  # noqa: S603
    except FileNotFoundError:
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)


if __name__ == '__main__':
    main()
