#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import Any

from dotenv import dotenv_values


APP_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = APP_ROOT / ".env"
HELPER_SOURCE = APP_ROOT / "scripts" / "ws_scraper_safe_reboot.sh"
SERVICE_SOURCE = APP_ROOT / "systemd" / "ws_scraper-scheduled-reboot.service"
TIMER_TEMPLATE_SOURCE = APP_ROOT / "systemd" / "ws_scraper-scheduled-reboot.timer.template"

HELPER_DEST = Path("/usr/local/sbin/ws_scraper_safe_reboot.sh")
SERVICE_DEST = Path("/etc/systemd/system/ws_scraper-scheduled-reboot.service")
TIMER_DEST = Path("/etc/systemd/system/ws_scraper-scheduled-reboot.timer")
TIMER_UNIT_NAME = "ws_scraper-scheduled-reboot.timer"


def normalize_env_scalar(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1].strip()
    return text


def parse_restart_schedule_s(raw_value: Any) -> int:
    text = normalize_env_scalar(raw_value)
    if not text:
        return 0
    try:
        value = int(text)
    except ValueError as exc:
        raise ValueError(f"RESTART_SCHEDULE_S must be an integer number of seconds, got {text!r}") from exc
    if value < 0:
        raise ValueError(f"RESTART_SCHEDULE_S must be >= 0, got {value}")
    return value


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def sudo_install(src: Path, dest: Path, mode: str) -> None:
    result = run(["sudo", "install", "-m", mode, str(src), str(dest)])
    if result.stdout.strip():
        print(result.stdout.strip())


def render_timer_unit(restart_schedule_s: int) -> str:
    template = TIMER_TEMPLATE_SOURCE.read_text(encoding="utf-8")
    return template.replace("__RESTART_SCHEDULE_S__", str(restart_schedule_s))


def current_uptime_seconds() -> float:
    raw = Path("/proc/uptime").read_text(encoding="utf-8").strip().split()[0]
    return float(raw)


def verify_prerequisites() -> None:
    missing: list[str] = []
    for path in (ENV_PATH, HELPER_SOURCE, SERVICE_SOURCE, TIMER_TEMPLATE_SOURCE):
        if not path.exists():
            missing.append(str(path))
    if missing:
        raise SystemExit(f"missing_required_files:{','.join(missing)}")
    result = run(["sudo", "-n", "true"], check=False)
    if result.returncode != 0:
        raise SystemExit("passwordless_sudo_required_for_install")


def install_rendered_files(restart_schedule_s: int) -> None:
    with tempfile.TemporaryDirectory(prefix="ws-scheduled-reboot-") as tmp_dir_raw:
        tmp_dir = Path(tmp_dir_raw)
        timer_path = tmp_dir / "ws_scraper-scheduled-reboot.timer"
        timer_path.write_text(render_timer_unit(restart_schedule_s), encoding="utf-8")
        sudo_install(HELPER_SOURCE, HELPER_DEST, "755")
        sudo_install(SERVICE_SOURCE, SERVICE_DEST, "644")
        sudo_install(timer_path, TIMER_DEST, "644")


def daemon_reload_and_verify() -> None:
    result = run(["sudo", "systemctl", "daemon-reload"])
    if result.stdout.strip():
        print(result.stdout.strip())
    verify = run(["systemd-analyze", "verify", str(SERVICE_DEST), str(TIMER_DEST)], check=False)
    if verify.returncode != 0:
        detail = (verify.stdout + "\n" + verify.stderr).strip()
        raise SystemExit(f"systemd_verify_failed:\n{detail}")


def enable_or_disable_timer(restart_schedule_s: int) -> None:
    if restart_schedule_s <= 0:
        run(["sudo", "systemctl", "disable", "--now", TIMER_UNIT_NAME], check=False)
        print(f"Disabled {TIMER_UNIT_NAME} because RESTART_SCHEDULE_S=0.")
        return

    enable = run(["sudo", "systemctl", "enable", TIMER_UNIT_NAME])
    if enable.stdout.strip():
        print(enable.stdout.strip())

    uptime_s = current_uptime_seconds()
    if uptime_s >= float(restart_schedule_s):
        print(
            "Timer enabled but not started live because current uptime already exceeds "
            f"RESTART_SCHEDULE_S ({int(uptime_s)}s >= {restart_schedule_s}s); "
            "starting it now would risk an immediate reboot. It will arm automatically on the next boot."
        )
        return

    start = run(["sudo", "systemctl", "start", TIMER_UNIT_NAME])
    if start.stdout.strip():
        print(start.stdout.strip())
    print(f"Enabled and started {TIMER_UNIT_NAME}.")


def print_status(restart_schedule_s: int) -> None:
    enabled = run(["systemctl", "is-enabled", TIMER_UNIT_NAME], check=False)
    active = run(["systemctl", "is-active", TIMER_UNIT_NAME], check=False)
    print(
        f"{TIMER_UNIT_NAME}: enabled={enabled.stdout.strip() or enabled.stderr.strip() or enabled.returncode} "
        f"active={active.stdout.strip() or active.stderr.strip() or active.returncode} "
        f"restart_schedule_s={restart_schedule_s}"
    )
    timers = run(["systemctl", "list-timers", "--all", "--no-pager", TIMER_UNIT_NAME], check=False)
    listing = timers.stdout.strip()
    if listing:
        print(listing)


def main() -> None:
    verify_prerequisites()
    env = dotenv_values(ENV_PATH)
    try:
        restart_schedule_s = parse_restart_schedule_s(env.get("RESTART_SCHEDULE_S"))
    except ValueError as exc:
        raise SystemExit(str(exc))

    install_rendered_files(restart_schedule_s)
    daemon_reload_and_verify()
    enable_or_disable_timer(restart_schedule_s)
    print_status(restart_schedule_s)


if __name__ == "__main__":
    main()
