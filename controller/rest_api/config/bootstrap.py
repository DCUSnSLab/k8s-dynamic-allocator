"""kda_config.py 의 값을 환경변수 기본값으로 올린다.

manage.py 와 wsgi.py 가 이 모듈을 공유한다. 전에는 manage.py 안에만 있어서 WSGI
서버로 띄우면 설정 파일이 아무 영향도 주지 않았다 - manage.py 를 거치지 않으니
--config-file 을 해석할 기회가 없다.

Django 설정을 읽기 전에 돌아야 하므로 django 를 import 하지 않는다. 값은 전부
setdefault 로 넣으므로, 이미 들어와 있는 환경변수가 항상 이긴다.
"""
import os
import runpy
from pathlib import Path


def config_value_to_env(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def load_config_defaults(config_file):
    """설정 파일의 대문자 이름들을 환경변수 기본값으로 올린다."""
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
            os.environ.setdefault(name, config_value_to_env(value))

    namespace = values.get("DEFAULT_NAMESPACE")
    if namespace:
        os.environ.setdefault("K8S_NAMESPACE", str(namespace))


def load_from_env():
    """KDA_CONFIG_FILE 이 가리키는 파일을 읽는다.

    WSGI 서버에는 명령행 인자를 넘길 자리가 없으므로 환경변수로 받는다. 변수가
    없거나 파일이 없으면 아무것도 하지 않는다 - 설정 파일 없이 환경변수만으로
    띄우는 것도 지원하는 구성이다.
    """
    config_file = os.environ.get("KDA_CONFIG_FILE")
    if not config_file:
        return
    if not Path(config_file).exists():
        return
    load_config_defaults(config_file)
