"""本地启动入口：uvicorn main:app --reload"""

from __future__ import annotations

from jobrunner.api import create_app
from jobrunner.builtin_handlers import register_builtins

register_builtins()

app = create_app()
