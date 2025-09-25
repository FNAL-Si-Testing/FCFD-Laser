#!/usr/bin/env python3
# fcfd_laser/daq/acquisition.py

import os
import json
import glob
import shutil
import logging
import subprocess
import time
from typing import List, Dict, Optional

from dotenv import load_dotenv
import pyvisa as visa


from ..utils import constants

def get_env_var():
    _here = os.path.dirname(os.path.abspath(__file__))

    _candidates = [
        os.getenv("FCFD_ENV_FILE"),                                 
        os.path.join(_here, "..", "..", "..", ".env"),              
        os.path.join(_here, "..", "..", ".env"),                    
        os.path.join(_here, "..", ".env"),                           
    ]

    dotenv_loaded = False
    for _p in [p for p in _candidates if p]:
        _p = os.path.normpath(_p)
        if os.path.isfile(_p):
            load_dotenv(dotenv_path=_p, override=True)
            dotenv_loaded = True
            break

    if not dotenv_loaded:
        _cur = _here
        for _ in range(5): 
            _probe = os.path.join(_cur, ".env")
            if os.path.isfile(_probe):
                load_dotenv(dotenv_path=_probe, override=True)
                dotenv_loaded = True
                break
            _parent = os.path.dirname(_cur)
            if _parent == _cur:
                break
            _cur = _parent

    return dotenv_loaded
_ENV_LOADED = get_env_var()

class LeCroyScope:
    """
    Minimal LeCroy controller used by your main orchestrator.

    Exposed API (only):
      - connect() -> bool
      - configure_from_file(config_path) -> bool
      - acquire_and_wait(timeout_s=30) -> bool
      - active_channels : List[int]
    """

    def __init__(self, ip_address: str = getattr(constants, "LECROY_IP", "192.168.0.170"),
                 logger: Optional[logging.Logger] = None):
        if logger is None:
            raise ValueError("logger must be provided")
        self.logger = logger
        self.ip = ip_address
        self.rm = visa.ResourceManager("@py")
        self.inst: Optional[visa.resources.MessageBasedResource] = None
        self.active_channels: List[int] = []
        self._trace_counter = 1  # Trace1, Trace2, ...
        self._segments = 1

    # ---- helpers (private) ----
    def _w(self, cmd: str) -> None:
        assert self.inst is not None, "Scope not connected"
        self.inst.write(cmd)

    def _q(self, cmd: str) -> str:
        assert self.inst is not None, "Scope not connected"
        return self.inst.query(cmd)

    # ---- public API ----
    def connect(self, timeout_ms: int = 300000) -> bool:
        try:
            self.inst = self.rm.open_resource(f"TCPIP0::{self.ip}::inst0::INSTR")
            self.inst.timeout = timeout_ms
            self.inst.encoding = "latin_1"
            self.inst.clear()
            self._w("COMM_HEADER OFF")
            self.logger.info(f"Connected to LeCroy @ {self.ip}")
            return True
        except visa.errors.VisaIOError as e:
            self.logger.error(f"VISA connect failed: {e}")
            self.inst = None
            return False
        
    def disconnect(self) -> None:
        """
        Resets the scope to real-time mode and closes the connection.
        """
        if self.inst:
            try:
                self.logger.info("Setting scope to Real Time mode (SEQ OFF).")
                self._w("SEQ OFF")
                self.logger.info("Disconnecting from scope.")
                self.inst.close()
            except Exception as e:
                self.logger.error(f"Error during disconnect: {e}")
            finally:
                self.inst = None
    def auto_setup(self) -> bool:
        """
        Tells the scope to perform an auto-setup to find optimal settings.
        This is useful for exploration but not for repeatable measurements.
        """
        if self.inst is None:
            self.logger.error("Scope not connected.")
            return False
        
        self.logger.info("Performing built-in auto-setup...")
        # The command for LeCroy is 'AUTO_SETUP' with the 'FIND' argument.
        self._w("AUTO_SETUP FIND")
        self._w("WAIT") # Wait for the operation to complete
        self.logger.info("Auto-setup complete.")
        return True

    def disconnect(self) -> None:
        if self.inst:
            try:
                self.logger.info("Setting scope to Real Time mode (SEQ OFF) and disconnecting.")
                self._w("SEQ OFF")
                self.inst.close()
            except Exception as e:
                self.logger.error(f"Error during disconnect: {e}")
            finally:
                self.inst = None

    def configure_from_file(self, config_path: str) -> bool:
        if self.inst is None:
            self.logger.error("Scope not connected. Cannot configure.")
            return False
        try:
            with open(config_path, "r") as f:
                cfg = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load scope config '{config_path}': {e}")
            return False

        self._w("STOP"); self._w("*CLS"); self._w("COMM_HEADER OFF")

        # --- Vertical Setup ---
        self.active_channels.clear()
        use_auto_vscale = cfg.get("use_auto_setup", False)

        if use_auto_vscale:
            self.logger.info("Auto-scaling vertical channels...")

        for ch, s in sorted(cfg.get("channels", {}).items(),
                            key=lambda kv: int(kv[0].replace("C", "").strip())):
            if not s.get("enabled", False):
                continue
            chn = ch.upper().strip()
            self.active_channels.append(int(chn.replace("C", "")))
            self._w(f"{chn}:TRACE ON")
            coupling = s.get("coupling", "D50")
            self._w(f"{chn}:COUPLING {coupling}")

            if use_auto_vscale:
                # Use the targeted command to find the vertical scale for THIS channel
                self._w(f"VBS 'app.Acquisition.{chn}.FindVerticalScale'")
            else:
                # Use manual settings from the file
                v_scale_v = float(s.get("v_scale", 0.05))
                v_pos_div = float(s.get("v_pos", 3.0))
                v_scale_mv = int(round(1000 * v_scale_v))
                v_offset_mv = int(round(1000 * v_scale_v * v_pos_div))
                self._w(f"{chn}:VOLT_DIV {v_scale_mv}MV")
                self._w(f"{chn}:OFFSET {v_offset_mv}MV")
        
        if use_auto_vscale:
             self._w("WAIT") # Wait for all FindVerticalScale operations to complete
             self.logger.info("Auto-scaling complete.")


        # --- Timebase and Trigger are ALWAYS set manually ---
        tb = cfg.get("timebase", {})
        horiz_ns = float(tb.get("horizontal_window_ns", 500))
        offs_ns  = float(tb.get("time_offset_ns", 0))
        time_div_ns = horiz_ns / 10.0
        self._w(f"TIME_DIV {time_div_ns}NS")
        self._w(f"TRIG_DELAY {int(offs_ns)} NS")
        self.logger.info(f"Applied Timebase: {time_div_ns:.3f} ns/div, delay {offs_ns:.0f} ns")

        trg = cfg.get("trigger", {})
        src = trg.get("source", "C1").upper()
        lvl = float(trg.get("level_v", 1.5))
        slope = str(trg.get("slope", "POSitive"))
        holdoff_ns = int(trg.get("holdoff_ns", 400))
        if holdoff_ns > 0:
            self._w(f"TRIG_SELECT Edge,SR,{src},HT,TI,HV,{float(holdoff_ns)} NS")
        else:
            self._w(f"TRIG_SELECT Edge,SR,{src},HT,OFF")
        if src != "LINE":
            self._w(f"{src}:TRLV {lvl:.6f}V")
            self._w(f"{src}:TRSL {slope}")
            self._w(f"TRIG_SLOPE {slope}")
        self.logger.info(f"Applied Trigger: {src}, {lvl} V, {slope}, holdoff {holdoff_ns} ns")

        # --- Acquisition Setup ---
        acq = cfg.get("acquisition", {})
        self._w(f"BANDWIDTH_LIMIT {acq.get('bandwidth_limit', 'OFF')}")
        self._segments = int(acq.get("segments", 1000))
        self.logger.info(f"Applied Acquisition: {self._segments} segments.")

        self._w("STORE_SETUP ALL_DISPLAYED,HDD,AUTO,OFF,FORMAT,BINARY")
        self.logger.info("Scope configuration complete.")
        return True

    def acquire_and_wait(self, timeout_s: int = 30) -> bool:
        if self.inst is None:
            self.logger.error("Scope not connected.")
            return False

        # Sequence ON/OFF
        if self._segments > 1:
            self._w(f"SEQ ON,{self._segments}")
        else:
            self._w("SEQ OFF")

        # Arm + block
        self._w("*TRG")
        self._w("WAIT")          # blocking like your working script
        _ = self._q("ALST?")     # sync/idle

        # Save ALL_DISPLAYED as Trace{counter} -> C#--Trace{counter}.trc
        n = self._trace_counter
        self._w(rf"""vbs 'app.SaveRecall.Waveform.TraceTitle="Trace{n}"' """)
        self._w(r"""vbs 'app.SaveRecall.Waveform.SaveFile' """)
        _ = self._q("ALST?")
        self._trace_counter += 1
        self.logger.info(f"Saved waveforms as Trace{n}.")
        return True


class ScopeFileTransfer:
    """
    Persistent CIFS mount + fast copies.

    Env:
      ARCADIA_PASSWORD  -> sudo password (for mount/umount)
      LECROY_PASSWORD   -> SMB share password for user 'lcrydmin'
    """

    def __init__(self, logger: logging.Logger,
                 scope: Optional[object] = None,
                 ip: Optional[str] = None,
                 mount_point: Optional[str] = None,
                 max_workers: Optional[int] = 8):
        self.logger = logger
        self.scope = scope
        self.ip = ip or getattr(scope, "ip", None) or getattr(constants, "LECROY_IP", "192.168.0.170")
        self.mount_point = mount_point or getattr(constants, "MOUNT_POINT", "/mnt")
        self.max_workers = max_workers
        self._own_mount = False

    def mount(self) -> bool:
        if self._mounted():
            self._own_mount = False
            self.logger.info(f"Waveforms already mounted at {self.mount_point}")
            return True

        sudo_pw = os.getenv("ARCADIA_PASSWORD", "")
        smb_pw  = os.getenv("LECROY_PASSWORD", "")
        if not sudo_pw:
            self.logger.error("ARCADIA_PASSWORD not set; cannot sudo mount.")
            return False
        if not smb_pw:
            self.logger.error("LECROY_PASSWORD not set; cannot authenticate to SMB share.")
            return False

        os.makedirs(self.mount_point, exist_ok=True)
        uid, gid = os.getuid(), os.getgid()
        for vers in ("3.0", "2.1", "1.0"):
            opts = (
                f"username=lcrydmin,password={smb_pw},vers={vers},uid={uid},gid={gid},"
                "rw,file_mode=0644,dir_mode=0755,iocharset=utf8,nounix"
            )
            cmd = ["sudo", "-S", "-p", "", "mount", "-t", "cifs",
                   f"//{self.ip}/Waveforms", self.mount_point, "-o", opts]
            res = subprocess.run(cmd, input=sudo_pw + "\n", text=True, capture_output=True)
            if res.returncode == 0:
                self._own_mount = True
                self.logger.info(f"Mounted Waveforms at {self.mount_point} (vers={vers})")
                return True
            err = (res.stderr or "").strip()
            if err:
                self.logger.warning(f"CIFS mount failed (vers={vers}): {err.splitlines()[-1]}")
        self.logger.error("Mount failed after all SMB version attempts.")
        return False

    def unmount(self) -> None:
        if not self._own_mount:
            return
        sudo_pw = os.getenv("ARCADIA_PASSWORD", "")
        if not sudo_pw:
            self.logger.warning("ARCADIA_PASSWORD missing during unmount; leaving mount as-is.")
            return
        cmd = ["sudo", "-S", "-p", "", "umount", self.mount_point]
        subprocess.run(cmd, input=sudo_pw + "\n", text=True, capture_output=True)
        self.logger.info(f"Unmounted {self.mount_point}")
        self._own_mount = False

    def _mounted(self) -> bool:
        try:
            with open("/proc/mounts", "r") as f:
                n1 = f"//{self.ip}/Waveforms"; n2 = f" {self.mount_point} "
                return any(n1 in ln and n2 in ln for ln in f)
        except Exception:
            return False

    def copy_trace(self, trace_num: int, dest_dir: str, cleanup: bool = True) -> int:
        """Build patterns from scope.active_channels and copy them."""
        chans = getattr(self.scope, "active_channels", []) or []
        patterns = [f"C{ch}--Trace{trace_num}.trc" for ch in chans]
        return self.copy_patterns(patterns=patterns, dest_dir=dest_dir, cleanup=cleanup)

    def copy_patterns(self, patterns: List[str], dest_dir: str, cleanup: bool = True) -> int:
        if not self._mounted():
            self.logger.error("Waveforms share is not mounted.")
            return 0
        os.makedirs(dest_dir, exist_ok=True)

        files: List[str] = []
        for pat in patterns:
            files.extend(glob.glob(os.path.join(self.mount_point, pat)))
            files.extend(glob.glob(os.path.join(self.mount_point, "**", pat), recursive=True))
        files = sorted({f for f in files if os.path.isfile(f)})

        if not files:
            self.logger.warning(f"No files matched on scope (patterns={patterns}).")
            return 0

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _copy_one(src: str) -> bool:
            try:
                dst = os.path.join(dest_dir, os.path.basename(src))
                tmp = dst + ".part"
                shutil.copy2(src, tmp)       
                os.replace(tmp, dst)       
                if cleanup:
                    try: os.remove(src)
                    except Exception as e: self.logger.warning(f"Delete failed for {src}: {e}")
                return True
            except Exception as e:
                self.logger.error(f"Copy failed for {src}: {e}")
                return False

        copied = 0
        with ThreadPoolExecutor(max_workers=min(8, len(files))) as ex:
            for fut in as_completed({ex.submit(_copy_one, f): f for f in files}):
                if fut.result():
                    copied += 1
        self.logger.info(f"Copied {copied} files from scope to {dest_dir}")
        return copied
