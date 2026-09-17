from os import getenv
from typing import Any


def _float_env(name: str, default: float, _getenv: Any = getenv) -> float:
    return float(_getenv(name, default))


def _retry_after_seconds(period: str) -> int:
    if period.endswith("s"):
        try:
            return int(period[:-1])
        except ValueError:
            pass
    try:
        return int(period)
    except ValueError:
        return 60



