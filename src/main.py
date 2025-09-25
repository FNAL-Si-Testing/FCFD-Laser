# main.py

"""
Main script for running a scan.

This orchestrates the motors, daq, conversion, and preprocessing.
Fully autmated, parallelized, vectorized for max performance. 
"""

import os
import time
from datetime import datetime
import argparse
import json
import random
import glob
import multiprocessing
import threading

from fcfd_laser.motor.motortools import Motors
from fcfd_laser.motor.scan_patterns import PATTERNS, _coord_from_index

from fcfd_laser.daq import acquisition 
from fcfd_laser.processing.conversion import convert_run
from fcfd_laser.processing.preprocessing import run_preprocessor
from fcfd_laser.utils import constants, logger, monitor
from fcfd_laser.utils.evnthandler import *

NUM_CPU = multiprocessing.cpu_count()
NUM_CONVERSION_WORKERS = max(1, NUM_CPU // 2 - 1)
NUM_PREPROCESSING_WORKERS = max(1, NUM_CPU // 2)

SRC_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(SRC_DIR, os.pardir))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# ============ [Change: Add timestamp to run directory] ================
# power = ".0%"
# RUN_FINGERPRINT = f"Power_{power}"
# run_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
# RUN_ID = f"run_{run_datetime}_{RUN_FINGERPRINT}"

RUN_FINGERPRINT = "DEBUG_RUN"
RUN_ID = "DEBUG_RUN"

RUN_DIR = os.path.join(OUTPUT_DIR, RUN_ID)
LOG_DIR = os.path.join(RUN_DIR, "logs")
DATA_DIR_RAW = os.path.join(RUN_DIR, "raw")
DATA_DIR_CONV = os.path.join(RUN_DIR, "converted")
DATA_DIR_PROC = os.path.join(RUN_DIR, "processed")

PREPROCESSOR_EXECUTABLE = os.path.join(SRC_DIR, "cpp", "build", "NetScopeStandaloneDat2Root")
CONFIG_DIR = os.path.join(PROJECT_ROOT, "configs")
PREPROCESSOR_CONFIG = os.path.join(CONFIG_DIR, "LecroyScope_v11.config")

# Setup logger after defining LOG_DIR
logger = logger.get_logger(log_dir=LOG_DIR, fingerprint=RUN_FINGERPRINT)
print(f"Log file created at {LOG_DIR}")

# Make run directories
os.makedirs(DATA_DIR_RAW, exist_ok=True)
os.makedirs(DATA_DIR_CONV, exist_ok=True)
os.makedirs(DATA_DIR_PROC, exist_ok=True)

def motor_task(motors: Motors,
               dX: float, nX: int,
                dY: float, nY: int,
                dZ: float, nZ: int,
                wait_ms: int,
                home_X: float, home_Y: float, home_Z: float,
                pattern: str = "xz_serpentine"
                ):
    motors.move_home(X=home_X, Y=home_Y, wait_time=wait_ms)
    motors.move_XYZ(Z=home_Z, wait_time=wait_ms)

    prevX, prevY, prevZ = home_X, home_Y, home_Z
    total_steps = nX * nY * nZ

    for i, (ix, iy, iz) in enumerate(PATTERNS[pattern](nX, nY, nZ)):
        X, Y, Z = _coord_from_index(ix, iy, iz, dX, dY, dZ, home_X, home_Y, home_Z)
        rel_dX, rel_dY, rel_dZ = X - prevX, Y - prevY, Z - prevZ

        logger.info(f"Step {i+1}/{total_steps}: Moving From ({prevX}, {prevY}, {prevZ}) to -> ({X}, {Y}, {Z}).")
        motors.move_XYZ_R(dX=rel_dX, dY=rel_dY, dZ=rel_dZ, wait_time=wait_ms)

        prevX, prevY, prevZ = X, Y, Z
        time.sleep(wait_ms / 1000) # pause for second: 
    
    logger.info("Scanning with Motors complete. Returning motors to the home position.")
    motors.move_home(X=home_X, Y=home_Y, wait_time=wait_ms)
    motors.move_XYZ(Z=home_Z, wait_time=wait_ms)
    
def daq_task(scope, xfer, scan_num):
    logger.info(f"Acquiring data for SCAN: {scan_num}...")
    try: 
        scope.acquire_and_wait()
        xfer.copy_trace(trace_num=scan_num, dest_dir=DATA_DIR_RAW, cleanup=True)
    except Exception as e:
        logger.error(f"DAQ task failed: {e}")
        raise e

def motor_daq_task(args, conversion_queue):
    """
    Producer process: controls motors, triggers DAQ, and puts scan_num into a queue.
    """
    # Initialize motors
    if args.run_motors:
        motors = Motors(logger=logger)
        motors.initialize_devices()
        motors.move_home(X=args.home_X, Y=args.home_Y, wait_time=args.wait_ms)
        motors.move_XYZ(Z=args.home_Z, wait_time=args.wait_ms)
        prevX, prevY, prevZ = args.home_X, args.home_Y, args.home_Z
    else:
        motors = None

    # Initialize DAQ
    if args.run_daq:
        scope = acquisition.LeCroyScope(logger=logger)
        if not scope.connect(): raise Exception("Failed to connect to LeCroy Scope.")
        if not scope.configure_from_file(args.scope_config): raise Exception("Failed to configure LeCroy Scope.")
        xfer = acquisition.ScopeFileTransfer(logger=logger, scope=scope)
        if not xfer.mount(): raise Exception("Failed to mount LeCroy Waveforms share.")
    else:
        scope = None
        xfer = None

    total_scans = args.nX * args.nY * args.nZ
    scan_num = 1
    for i, (ix, iy, iz) in enumerate(PATTERNS[args.pattern](args.nX, args.nY, args.nZ)):
        if args.run_motors:
            X, Y, Z = _coord_from_index(ix, iy, iz, args.dX, args.dY, args.dZ, args.home_X, args.home_Y, args.home_Z)
            rel_dX, rel_dY, rel_dZ = X - prevX, Y - prevY, Z - prevZ
            logger.info(f"Step {i}/{total_scans}: Moving From ({prevX}, {prevY}, {prevZ}) to -> ({X}, {Y}, {Z}).")
            motors.move_XYZ_R(dX=rel_dX, dY=rel_dY, dZ=rel_dZ, wait_time=args.wait_ms)
            prevX, prevY, prevZ = X, Y, Z
            time.sleep(args.wait_ms / 1000) # Wait for second (stabilize the motor)

        if args.run_daq:
            logger.info(f"Acquiring data for SCAN: {scan_num}...")
            try:
                scope.acquire_and_wait()
                xfer.copy_trace(trace_num=scan_num, dest_dir=DATA_DIR_RAW, cleanup=True)
                # Put the scan number into the conversion queue
                conversion_queue.put(scan_num)
                scan_num += 1
            except Exception as e:
                logger.error(f"DAQ task failed: {e}")
                raise e
    if motors:
        logger.info("Scanning with Motors complete. Returning motors to the home position.")
        motors.move_home(X=args.home_X, Y=args.home_Y, wait_time=args.wait_ms)
        motors.move_XYZ(Z=args.home_Z, wait_time=args.wait_ms)
        motors.close_devices()
    
    # Signal that the DAQ process is done
    for _ in range(NUM_CONVERSION_WORKERS):
        conversion_queue.put(None)
        
def conversion_task(scan_num: int, channels: list):
    out_path = convert_run(
        raw_dir=DATA_DIR_RAW,
        scan_num=scan_num,
        channels=channels,
        out_dir=DATA_DIR_CONV,
        logger=logger
    )
    logger.info(f"Converted ROOT written to: {out_path}")
    return out_path

def conversion_task_consumer(conversion_queue, preprocessing_queue):
    """
    Consumer-Producer process: gets scan_num from a queue, runs conversion,
    and puts the output file path into another queue.
    """
    while True:
        scan_num = conversion_queue.get()
        if scan_num is None:
            break
        
        active_channels = [
            int(os.path.basename(p).split('--')[0][1:])
            for p in sorted(glob.glob(os.path.join(DATA_DIR_RAW, f"C*--Trace{scan_num}.trc")))
        ]
        
        if not active_channels:
            logger.warning(f"Conversion: No raw files found for scan {scan_num}. Skipping.")
            continue

        out_path = conversion_task(
            scan_num=scan_num,
            channels=active_channels,
        )
        # Put the path to the converted file into the preprocessing queue
        preprocessing_queue.put(out_path)
    
    # Signal that the conversion process is done
    for _ in range(NUM_PREPROCESSING_WORKERS):
        preprocessing_queue.put(None)

def processing_task_consumer(preprocessing_queue):
    """
    Consumer process: gets the path to a converted file from a queue and runs preprocessing.
    """
    while True:
        converted_file_path = preprocessing_queue.get()
        if converted_file_path is None:
            break

        processed_file_path = os.path.join(
            DATA_DIR_PROC,
            f"processed_{os.path.basename(converted_file_path)}"
        )
        
        run_preprocessor(
            logger=logger,
            converted_file_path=converted_file_path,
            processed_file_path=processed_file_path,
            config_file_path=PREPROCESSOR_CONFIG,
            executable_path=PREPROCESSOR_EXECUTABLE
        )

def parse_args():
    defaults = dict(
        dX=100.0, nX=1,
        dY=0.0,  nY=1,
        dZ=0.0,  nZ=1,
        wait_ms=100,
        home_X=0.0, home_Y=0.0, home_Z=0.0,
        pattern="xz_serpentine",
        run_motors = True,
        run_daq = True,
        run_conversion = True,
        run_preprocessing = True
    )

    p = argparse.ArgumentParser(
        description="Motorized X-Y-Z scanner with pluggable patterns",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--scope-config", type=str, default=os.path.join(CONFIG_DIR, "scope_config.json"), help="JSON file with oscilloscope configuration.")
    p.add_argument("--scan-config", type=str, default=os.path.join(CONFIG_DIR, "scan_config.json"), help="JSON file with scan configuration. Overrides defaults and CLI args.")
    p.add_argument("--dX", type=float, default=defaults["dX"], help="X step (um)")
    p.add_argument("--nX", type=int,   default=defaults["nX"], help="number of X steps")
    p.add_argument("--dY", type=float, default=defaults["dY"], help="Y step (um)")
    p.add_argument("--nY", type=int,   default=defaults["nY"], help="number of Y steps")
    p.add_argument("--dZ", type=float, default=defaults["dZ"], help="Z step (um)")
    p.add_argument("--nZ", type=int,   default=defaults["nZ"], help="number of Z steps")
    p.add_argument("--wait-ms", type=int, default=defaults["wait_ms"], help="wait time after each move (ms)")
    p.add_argument("--home-X", type=float, default=defaults["home_X"], help="home X (um)")
    p.add_argument("--home-Y", type=float, default=defaults["home_Y"], help="home Y (um)")
    p.add_argument("--home-Z", type=float, default=defaults["home_Z"], help="home Z (um)")
    p.add_argument("--pattern", type=str, default=defaults["pattern"], help=f"scan order: {list(PATTERNS.keys())}")
    p.add_argument("--run-motors", action="store_true", default=defaults["run_motors"])
    p.add_argument("--run-daq", action="store_true", default=defaults["run_daq"])
    p.add_argument("--run-conversion", action="store_true", default=defaults["run_conversion"])
    p.add_argument("--run-preprocessing", action="store_true", default=defaults["run_preprocessing"])
    args = p.parse_args()

    if args.scan_config:
        with open(args.scan_config, "r") as f:
            cfg = json.load(f)
        for k, v in cfg.items():
            if hasattr(args, k):
                setattr(args, k, v)

    return args

def main():
    args = parse_args()

    logger.info(
        f"\n=============== Starting Scan (Fingerprint: {RUN_FINGERPRINT})=============== "
        f"\nscan3d_start pattern={args.pattern} "
        f"\ndX={args.dX} nX={args.nX} dY={args.dY} nY={args.nY} dZ={args.dZ} nZ={args.nZ} "
        f"\nhome_X={args.home_X} home_Y={args.home_Y} home_Z={args.home_Z} "
        f"\nwait={args.wait_ms}ms run_motors={args.run_motors} run_daq={args.run_daq} run_conversion={args.run_conversion}"
        f"\n============================================================================== \n"
    )

    # Queues
    conversion_queue = multiprocessing.Queue()
    preprocessing_queue = multiprocessing.Queue()

    # Processes
    daq_process = multiprocessing.Process(
        name="DAQ_Process",
        target=motor_daq_task,
        args=(args, conversion_queue)
    )
    
    conversion_processes = [
        multiprocessing.Process(
            name=f"Conversion_{i}",
            target=conversion_task_consumer,
            args=(conversion_queue, preprocessing_queue)
        )
        for i in range(NUM_CONVERSION_WORKERS)
    ]

    preprocessing_processes = [
        multiprocessing.Process(
            name=f"Preprocessing_{i}",
            target=processing_task_consumer,
            args=(preprocessing_queue,)
        )
        for i in range(NUM_PREPROCESSING_WORKERS)
    ]

    all_processes = [daq_process] + conversion_processes + preprocessing_processes
    monitor_thread = threading.Thread(
        target=monitor.monitor_queues,
        args=({
            "Conversion Queue": conversion_queue,
            "Preprocessing Queue": preprocessing_queue
        }, all_processes, logger),
        daemon=True # A daemon thread will exit when the main program exits
    )
    
    if args.run_daq:
        daq_process.start()
    if args.run_conversion:
        for p in conversion_processes:
            p.start()
    if args.run_preprocessing:
        for p in preprocessing_processes:
            p.start()

    monitor_thread.start()

    # Join processes
    if args.run_daq:
        daq_process.join()
    if args.run_conversion:
        for p in conversion_processes:
            p.join()
    if args.run_preprocessing:
        for p in preprocessing_processes:
            p.join()
    monitor_thread.join()
    
    logger.info("All tasks completed.")


if __name__ == "__main__":
    main()
    print("\a")