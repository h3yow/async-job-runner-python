"""本地启动入口：uvicorn main:app --reload"""

from jobrunner.api import create_app

app = create_app()
