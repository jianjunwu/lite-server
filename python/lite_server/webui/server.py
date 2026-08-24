"""Entry point for `lite-server web`: serve the SPA and proxy instances."""

from __future__ import annotations

import logging
import os
from importlib import resources
from logging.handlers import RotatingFileHandler

import uvicorn

from .app import build_app
from .auth import UserStore
from .config import InstanceStore

_logger = logging.getLogger("lite_server.webui")


def _bundled_dist() -> str | None:
    """SPA built into the wheel as package data (lite_server/webui/static/)."""
    try:
        dist = resources.files("lite_server.webui") / "static"
        if (dist / "index.html").is_file():
            return str(dist)
    except (FileNotFoundError, ModuleNotFoundError):
        pass
    return None


def configure_audit_logging(env) -> None:
    """Optional rotating file sink for the audit logger. Audit rows always
    land in SQLite regardless; this adds a file mirror when configured."""
    log_path = env.get("LITE_UI_AUDIT_LOG")
    if not log_path:
        return
    handler = RotatingFileHandler(
        log_path,
        maxBytes=int(env.get("LITE_UI_AUDIT_LOG_MAX_BYTES", str(10 * 1024 * 1024))),
        backupCount=int(env.get("LITE_UI_AUDIT_LOG_BACKUPS", "5")),
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    audit_logger = logging.getLogger("lite_server.webui.audit")
    audit_logger.addHandler(handler)
    audit_logger.setLevel(logging.INFO)


def run_web(
    *,
    host: str | None = None,
    port: int | None = None,
    instances_file: str | None = None,
    auth_file: str | None = None,
    auth: str | None = None,
    web_dist: str | None = None,
) -> None:
    env = os.environ
    host = host or env.get("LITE_UI_HOST", "0.0.0.0")
    port = port or int(env.get("LITE_UI_PORT", "8600"))
    # Mutable state files default to the working directory.
    instances_path = instances_file or env.get("LITE_UI_INSTANCES_FILE") or "instances.yaml"
    auth_db = env.get("LITE_UI_AUTH_DB") or "auth.db"
    # auth.yaml is kept only as a one-shot import source for pre-SQLite state.
    auth_path = auth_file or env.get("LITE_UI_AUTH_FILE") or "auth.yaml"
    auth_enabled = (auth or ("off" if env.get("LITE_UI_AUTH") == "false" else "on")) == "on"
    web_dist = web_dist or env.get("LITE_UI_WEB_DIST") or _bundled_dist()

    if web_dist is None:
        _logger.warning(
            "web UI assets not found; serving API only "
            "(run `pnpm -C ui/web build` or pass --web-dist)"
        )

    registry = InstanceStore(instances_path, env)
    user_store = UserStore(auth_db, env, legacy_auth_path=auth_path)
    configure_audit_logging(env)
    app = build_app(registry, web_dist=web_dist, user_store=user_store, auth_enabled=auth_enabled)

    _logger.info(
        "lite-server web listening on http://%s:%d, %d instance(s), auth %s",
        host, port, len(registry.list()), "on" if auth_enabled else "OFF",
    )
    # proxy_headers makes uvicorn honor X-Forwarded-Proto/-For (Secure cookie
    # flag, throttling and audit IPs). Only enable behind a trusted reverse
    # proxy — direct deployments must leave it off or clients can spoof IPs.
    uvicorn.run(app, host=host, port=port,
                proxy_headers=env.get("LITE_UI_PROXY_HEADERS") == "true")
