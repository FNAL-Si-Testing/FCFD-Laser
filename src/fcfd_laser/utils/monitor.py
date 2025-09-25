# fcfd_laser/utils/monitor.py

import time
import logging

def monitor_queues(queues: dict, processes: list, logger: logging.Logger, interval: float = 0.25):
    """
    Periodically prints queue sizes and watches process liveness.
    - Waits until at least one process has started (pid assigned) before reporting.
    - Exits when all started processes have terminated.
    """
    logger.info("Monitor thread started.")
    try:
        any_started = False
        while True:
            # detect start/finish state
            pids = [p.pid for p in processes]
            lives = [p.is_alive() for p in processes]
            if any(pid is not None for pid in pids):
                any_started = True

            # status line
            status_lines = []
            for name, q in queues.items():
                try:
                    size = q.qsize()  # approximate, may raise on some platforms
                    status_lines.append(f"{name}: {size}")
                except Exception:
                    status_lines.append(f"{name}: [n/a]")

            # only print after something actually started to avoid “instant finish”
            if any_started:
                print("STATUS | " + " | ".join(status_lines), end="\r", flush=True)

            # exit condition: once at least one started and none are alive
            if any_started and not any(lives):
                break

            time.sleep(interval)
    finally:
        logger.info("Monitor thread finished.")
        print()  # newline after the last carriage-returned status line
