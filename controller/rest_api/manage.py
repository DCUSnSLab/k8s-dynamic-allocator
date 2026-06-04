#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import runpy
import sys
from pathlib import Path


def _config_value_to_env(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


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


def _load_config_defaults(config_file):
    if not config_file:
        return

    config_path = Path(config_file)
    if not config_path.is_absolute():
        config_path = Path.cwd() / config_path

    values = runpy.run_path(str(config_path))
    os.environ.setdefault("KDA_CONFIG_FILE", str(config_path))

    for name, value in values.items():
        if not name.isupper() or value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            os.environ.setdefault(name, _config_value_to_env(value))

    namespace = values.get("DEFAULT_NAMESPACE")
    if namespace:
        os.environ.setdefault("K8S_NAMESPACE", str(namespace))


def main():
    """Run administrative tasks."""
    _load_config_defaults(_extract_config_file(sys.argv))
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
