from __future__ import annotations
import importlib
import importlib.util
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from pathlib import Path

from lib.common.config_handler import load_config
from lib.common.logging import setup_logging, get_logger

_stop = False


def _on_signal(sig, frame):
    global _stop
    _stop = True


def _parse_time_str(t: str) -> tuple[int, int]:
    parts = t.split(":")
    if len(parts) != 2:
        raise ValueError("invalid time format, expected HH:MM")
    return int(parts[0]) % 24, int(parts[1]) % 60


def _next_run_dt(hour: int, minute: int, tzinfo) -> datetime:
    now = datetime.now(tzinfo)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return target


def _sleep_until(dt: datetime) -> bool:
    """Schlafe bis dt oder bis Shutdown. Return True wenn ausgelaufen, False wenn abgebrochen."""
    global _stop
    while not _stop:
        now = datetime.now(dt.tzinfo)
        secs = (dt - now).total_seconds()
        if secs <= 0:
            return True
        time.sleep(min(secs, 1.0))
    return False


def _import_module_by_candidates(project_root: Path, candidates: list[str], logger) -> object | None:
    """
    Versucht, ein Modul zuerst per importlib.import_module zu laden (Package-Import),
    falls das fehlschlägt, versucht es den direkten Dateipfad (spec_from_file_location).
    Liefert das Modulobjekt oder None.
    """
    # Versuche Package-Imports zuerst
    for mod_name in candidates:
        try:
            logger.debug("Trying importlib.import_module('%s')", mod_name)
            mod = importlib.import_module(mod_name)
            return mod
        except Exception as e:
            logger.debug("import_module('%s') failed: %s", mod_name, e)

    # Versuche Datei-Imports: suche passende .py unter project_root und project_root / scr
    search_dirs = [project_root, project_root / "scr"]
    for mod_name in candidates:
        base = mod_name.split(".")[-1]
        filename = base + ".py"
        for d in search_dirs:
            path = d / filename
            if path.exists():
                try:
                    logger.debug("Loading module from file %s", path)
                    spec = importlib.util.spec_from_file_location(mod_name, str(path))
                    if spec and spec.loader:
                        mod = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(mod)  # type: ignore
                        return mod
                except Exception as e:
                    logger.exception("Failed to load module from %s: %s", path, e)
    return None


def _run_crawler(logger, project_root: Path) -> None:
    try:
        candidates = [
            "scr.german_newspaper_crawler",
            "scr.german_newspapaer_crawler",  # möglichen Tippfehler abdecken
            "german_newspaper_crawler",
            "german_newspapaer_crawler",
        ]
        mod = _import_module_by_candidates(project_root, candidates, logger)
        if mod is None:
            logger.error("Keine passende Crawler-Moduldatei gefunden. Erwartete eines von: %s", candidates)
            return

        crawler_main = getattr(mod, "main", None)
        if callable(crawler_main):
            logger.info("Starting crawler run")
            rc = crawler_main()
            logger.info("Crawler finished with return code %s", rc)
        else:
            logger.error("crawler module has no callable main()")
    except Exception:
        logger.exception("Error while running crawler")


def main() -> int:
    global _stop
    try:
        cfg = load_config() or {}

        # bestimme projekt-root und default logs-verzeichnis: <project>/lib/common/logs
        project_root = Path(__file__).resolve().parents[1]
        default_logs_dir = project_root / "lib" / "common" / "logs"

        # make project root importable so package-imports work
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))

        logging_cfg = cfg.get("logging", {}) if isinstance(cfg, dict) else {}
        logging_cfg.setdefault("logdir", str(default_logs_dir))

        # setup_logging erwartet einen Level-String
        setup_logging(logging_cfg.get("level", "INFO"))
        logger = get_logger(__name__)

        # Erlaube scheduler-Konfiguration entweder top-level oder unter downloads.scheduler
        sched_cfg = {}
        if isinstance(cfg, dict):
            sched_cfg = cfg.get("scheduler") or {}
            if not isinstance(sched_cfg, dict):
                sched_cfg = {}
            if not sched_cfg:
                downloads = cfg.get("downloads") if isinstance(cfg.get("downloads"), dict) else {}
                sched_cfg = downloads.get("scheduler") if isinstance(downloads.get("scheduler"), dict) else {}

        # sofortiger einmaliger Lauf beim Start (unabhängig von enabled)
        if not _stop:
            logger.info("Initial run on scheduler start")
            _run_crawler(logger, project_root)
            if _stop:
                logger.info("Shutdown requested after initial run")
                logger.info("Scheduler shutting down")
                return 0

        # falls Scheduling nicht aktiviert ist, nach dem Initiallauf beenden
        if not sched_cfg.get("enabled", False):
            logger.info("Scheduler disabled in config; nothing to do.")
            return 0

        time_str = sched_cfg.get("time", "01:00")
        tz_name = sched_cfg.get("timezone")

        try:
            tz = ZoneInfo(tz_name) if tz_name else datetime.now().astimezone().tzinfo or timezone.utc
        except (ZoneInfoNotFoundError, Exception):
            logger.warning("Timezone '%s' not available, falling back to UTC", tz_name)
            tz = timezone.utc

        hour, minute = _parse_time_str(time_str)

        signal.signal(signal.SIGINT, _on_signal)
        signal.signal(signal.SIGTERM, _on_signal)

        logger.info("Scheduler started, daily at %02d:%02d (%s)", hour, minute, getattr(tz, "key", str(tz)))

        while not _stop:
            next_dt = _next_run_dt(hour, minute, tz)
            logger.info("Next run scheduled at %s", next_dt.isoformat())
            if not _sleep_until(next_dt):
                break  # interrupted

            if _stop:
                break

            _run_crawler(logger, project_root)

        logger.info("Scheduler shutting down")
        return 0
    except Exception:
        get_logger(__name__).exception("Unhandled exception in scheduler")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
