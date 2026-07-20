from __future__ import annotations

import argparse
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from types import TracebackType
from typing import TextIO


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_BOT_PATH = BASE_DIR / "bot.py"
SUPERVISOR_LOG_PATH = BASE_DIR / "supervisor.log"
SUPERVISOR_LOCK_PATH = BASE_DIR / "supervisor.lock"


def load_env_file(path: Path = BASE_DIR / ".env") -> None:
    if not path.is_file():
        return
    with path.open(encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            key, separator, value = line.partition("=")
            key = key.strip()
            if not separator or not key or key in os.environ:
                continue
            value = value.strip()
            if (
                len(value) >= 2
                and value[0] == value[-1]
                and value[0] in {"'", '"'}
            ):
                value = value[1:-1]
            os.environ[key] = value


def get_env_float(name: str, default: float, minimum: float) -> float:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        return max(minimum, float(raw_value))
    except ValueError:
        raise SystemExit(f"{name} must be a number, got {raw_value!r}")


load_env_file()


RESTART_DELAY_SECONDS = get_env_float(
    "SUPERVISOR_RESTART_DELAY_SECONDS", 5.0, 0.1
)
MAX_RESTART_DELAY_SECONDS = get_env_float(
    "SUPERVISOR_MAX_RESTART_DELAY_SECONDS", 60.0, RESTART_DELAY_SECONDS
)
STABLE_RUN_SECONDS = get_env_float(
    "SUPERVISOR_STABLE_RUN_SECONDS", 300.0, 1.0
)
SHUTDOWN_TIMEOUT_SECONDS = get_env_float(
    "SUPERVISOR_SHUTDOWN_TIMEOUT_SECONDS", 20.0, 1.0
)


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("bob-supervisor")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s]: %(message)s"
    )
    file_handler = RotatingFileHandler(
        SUPERVISOR_LOG_PATH,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


LOGGER = configure_logging()
STOP_EVENT = threading.Event()


class SupervisorAlreadyRunningError(RuntimeError):
    pass


class SupervisorLock:
    def __init__(self, path: Path = SUPERVISOR_LOCK_PATH):
        self.path = path
        self.handle: TextIO | None = None

    def __enter__(self) -> SupervisorLock:
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0, os.SEEK_END)
                if self.handle.tell() == 0:
                    self.handle.write("0")
                    self.handle.flush()
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as error:
            self.handle.close()
            self.handle = None
            raise SupervisorAlreadyRunningError(
                f"Another supervisor is already using {self.path}"
            ) from error

        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(str(os.getpid()))
        self.handle.flush()
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def request_shutdown(signum: int, _frame: object) -> None:
    signal_name = signal.Signals(signum).name
    LOGGER.info("Received %s; stopping supervisor.", signal_name)
    STOP_EVENT.set()


def install_signal_handlers() -> None:
    for signal_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        shutdown_signal = getattr(signal, signal_name, None)
        if shutdown_signal is not None:
            signal.signal(shutdown_signal, request_shutdown)


def log_bot_output(stream: TextIO) -> None:
    try:
        for raw_line in stream:
            line = raw_line.rstrip("\r\n")
            if not line:
                continue
            upper_line = line.upper()
            if any(
                marker in upper_line
                for marker in ("CRITICAL", "ERROR", "EXCEPTION", "TRACEBACK")
            ):
                LOGGER.error("[bot] %s", line)
            elif "WARNING" in upper_line:
                LOGGER.warning("[bot] %s", line)
            else:
                LOGGER.info("[bot] %s", line)
    except (OSError, ValueError):
        if not STOP_EVENT.is_set():
            LOGGER.exception("Failed while reading bot output.")
    finally:
        try:
            stream.close()
        except (AttributeError, OSError):
            pass


def launch_bot(bot_path: Path) -> subprocess.Popen[str]:
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"

    creation_flags = 0
    if os.name == "nt":
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP

    return subprocess.Popen(
        [sys.executable, "-u", str(bot_path)],
        cwd=BASE_DIR,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=creation_flags,
        start_new_session=os.name != "nt",
    )


def stop_bot(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return

    LOGGER.info("Stopping bot process %s.", process.pid)
    try:
        if os.name == "nt":
            try:
                process.send_signal(signal.CTRL_BREAK_EVENT)
            except (OSError, ValueError):
                process.terminate()
        else:
            os.killpg(process.pid, signal.SIGINT)
        process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        LOGGER.error(
            "Bot did not stop within %.1f seconds; forcing termination.",
            SHUTDOWN_TIMEOUT_SECONDS,
        )
        if os.name == "nt":
            process.kill()
        else:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    except ProcessLookupError:
        pass


def supervise(bot_path: Path, restart: bool = True) -> int:
    if not bot_path.is_file():
        LOGGER.critical("Bot file does not exist: %s", bot_path)
        return 2

    restart_delay = RESTART_DELAY_SECONDS
    restart_count = 0

    while not STOP_EVENT.is_set():
        try:
            process = launch_bot(bot_path)
        except OSError:
            LOGGER.exception("Unable to launch %s.", bot_path)
            return 2

        started_at = time.monotonic()
        restart_count += 1
        LOGGER.info(
            "Started bot process %s (launch #%s) with %s.",
            process.pid,
            restart_count,
            sys.executable,
        )

        output_thread = threading.Thread(
            target=log_bot_output,
            args=(process.stdout,),
            name="bot-output-reader",
            daemon=True,
        )
        output_thread.start()

        while process.poll() is None and not STOP_EVENT.wait(0.5):
            pass

        if STOP_EVENT.is_set():
            stop_bot(process)

        exit_code = process.wait()
        output_thread.join(timeout=2.0)
        runtime = time.monotonic() - started_at

        if STOP_EVENT.is_set():
            LOGGER.info("Bot stopped with exit code %s; supervisor is exiting.", exit_code)
            return 0

        if not restart:
            LOGGER.info("Bot exited with code %s after %.1f seconds.", exit_code, runtime)
            return exit_code

        if runtime >= STABLE_RUN_SECONDS:
            restart_delay = RESTART_DELAY_SECONDS

        LOGGER.error(
            "Bot exited with code %s after %.1f seconds; restarting in %.1f seconds.",
            exit_code,
            runtime,
            restart_delay,
        )
        if STOP_EVENT.wait(restart_delay):
            return 0

        if runtime < STABLE_RUN_SECONDS:
            restart_delay = min(restart_delay * 2, MAX_RESTART_DELAY_SECONDS)

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch and supervise the public Bob bot process"
    )
    parser.add_argument(
        "--bot",
        type=Path,
        default=DEFAULT_BOT_PATH,
        help="bot file to supervise (default: bot.py beside this file)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="run the bot once without restarting it",
    )
    return parser.parse_args()


def main() -> int:
    STOP_EVENT.clear()
    args = parse_args()
    bot_path = args.bot
    if not bot_path.is_absolute():
        bot_path = BASE_DIR / bot_path

    try:
        with SupervisorLock():
            install_signal_handlers()
            LOGGER.info("Supervisor started for %s.", bot_path.resolve())
            try:
                return supervise(bot_path.resolve(), restart=not args.once)
            finally:
                LOGGER.info("Supervisor stopped.")
    except SupervisorAlreadyRunningError as error:
        LOGGER.critical("%s", error)
        return 2
    except KeyboardInterrupt:
        STOP_EVENT.set()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
