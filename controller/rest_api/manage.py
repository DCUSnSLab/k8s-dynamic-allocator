#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys

from config.bootstrap import load_config_defaults, load_from_env


def _extract_config_file(argv):
    config_file = None
    cleaned = [argv[0]]
    index = 1

    while index < len(argv):
        arg = argv[index]
        if arg == "--config-file":
            if index + 1 >= len(argv):
                raise SystemExit("--config-file requires a path")
            config_file = argv[index + 1]
            index += 2
            continue
        if arg.startswith("--config-file="):
            config_file = arg.split("=", 1)[1]
            index += 1
            continue

        cleaned.append(arg)
        index += 1

    argv[:] = cleaned
    return config_file


def main():
    """Run administrative tasks."""
    config_file = _extract_config_file(sys.argv)
    if config_file:
        load_config_defaults(config_file)
    else:
        # WSGI 서버와 같은 경로. --config-file 없이 띄워도 KDA_CONFIG_FILE 이
        # 있으면 같은 설정을 읽는다.
        load_from_env()
    # 리로더 없이 띄우는 runserver 에는 RUN_MAIN 이 없으므로, 서버라는 사실을
    # 여기서 알려 준다. 다른 관리 명령에서는 올리지 않는다.
    if "runserver" in sys.argv and "--noreload" in sys.argv:
        os.environ.setdefault('KDA_START_ORCHESTRATOR', 'true')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
