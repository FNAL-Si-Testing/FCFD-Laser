import numpy as np
import uproot
import os
import pandas as pd

import sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from fcfd_laser.processing import amptime


LP2_50_COLS = dict(laser=0, strip1=4, strip2=5, strip3=6)

def ultra_processed_columns(
        converted_file_path: str,
        processed_file_path: str,
        analog_channels: list[int],
        channel_map: dict[int, int],
        run_id: int,
        x_um: float,
        y_um: float,
        z_um: float,
        n_events: int = 2500,
        lp2_cols: dict = None,
    ) -> dict:

    lp2_cols = lp2_cols or LP2_50_COLS

    with uproot.open(processed_file_path) as data_file:
        amp_data = data_file["pulse;1"]['amp'].arrays(library="np")['amp']
        baseline_data = data_file["pulse;1"]['baseline'].arrays(library="np")['baseline']
        LP2_50_data = data_file["pulse;1"]['LP2_50'].arrays(library="np")['LP2_50'] * 1e9

    # n_file_events = LP2_50_data.shape[0]
    # amps = amptime.process_file_matrix(
    #     converted_file_path, analog_channels, channel_map, n_events=n_events
    # )
    amps = amptime.process_amp_fast(converted_file_path, analog_channels)

    laser = LP2_50_data[:, lp2_cols["laser"]]
    strip1 = LP2_50_data[:, lp2_cols["strip1"]]
    strip2 = LP2_50_data[:, lp2_cols["strip2"]]
    strip3 = LP2_50_data[:, lp2_cols["strip3"]]

    dt1 = strip1 - laser
    dt2 = strip2 - laser
    dt3 = strip3 - laser

    run_id_arr   = np.full(n_events, run_id,  dtype=np.int32)
    x_arr = np.full(n_events, x_um, dtype=np.float32)
    y_arr = np.full(n_events, y_um, dtype=np.float32)
    z_arr = np.full(n_events, z_um, dtype=np.float32)

    # cast to compact types
    out = {
        "run_id":   run_id_arr,
        "x_um":     x_arr,
        "y_um":     y_arr,
        "z_um":     z_arr,

        "analog_1":     amps[:, 0].astype(np.float32, copy=False),
        "analog_2":     amps[:, 1].astype(np.float32, copy=False) if amps.shape[1] > 1 else np.full(n_events, np.nan, np.float32),
        "analog_3":     amps[:, 2].astype(np.float32, copy=False) if amps.shape[1] > 2 else np.full(n_events, np.nan, np.float32),
        
        "amp_1": amp_data[:, 1].astype(np.float32, copy=False),
        "amp_2": amp_data[:, 2].astype(np.float32, copy=False),
        "amp_3": amp_data[:, 3].astype(np.float32, copy=False),

        "baseline_amp_1": baseline_data[:, 1].astype(np.float32, copy=False),
        "baseline_amp_2": baseline_data[:, 2].astype(np.float32, copy=False),
        "baseline_amp_3": baseline_data[:, 3].astype(np.float32, copy=False),

        "t_laser":    laser.astype(np.float32, copy=False),
        "t_strip_1":   strip1.astype(np.float32, copy=False),
        "t_strip_2":   strip2.astype(np.float32, copy=False),
        "t_strip_3":   strip3.astype(np.float32, copy=False),

        "dt_strip_1":      dt1.astype(np.float32, copy=False),
        "dt_strip_2":      dt2.astype(np.float32, copy=False),
        "dt_strip_3":      dt3.astype(np.float32, copy=False),

        # "t_power_meter": LP2_50_data[:, 7].astype(np.float32, copy=False),
        # "dt_power_meter": (LP2_50_data[:, 7] - laser).astype(np.float32, copy=False),
        "amp_power_meter": amp_data[:, 7].astype(np.float32, copy=False),
        "baseline_power_meter": baseline_data[:, 7].astype(np.float32, copy=False),
    }

    return out

def save_to_root(cols: dict, out_dir: str, run_id: int, x_um: float, y_um: float, z_um: float, tree_name: str = "pulse_summary"):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.DataFrame(cols)
    fname = f"run_{run_id:04d}_X{int(x_um)}_Y{int(y_um)}_Z{int(z_um)}.root"
    out_file = os.path.join(out_dir, fname)
    with uproot.recreate(out_file) as f:
        f[tree_name] = df
    print(f"[✓] Saved ROOT: {out_file}")

if __name__ == "__main__":
    import glob
    import re

    channel_map = {
    1: 4,  # Ana Ch 2 (idx 1) -> Dig Ch 5 (idx 4)
    2: 5,  # Ana Ch 3 (idx 2) -> Dig Ch 6 (idx 5)
    3: 6,  # Ana Ch 4 (idx 3) -> Dig Ch 7 (idx 6)
    }
    analog_channels = sorted(list(channel_map.keys()))
    digital_channels = sorted(list(channel_map.values()))

    run_dir = '/home/arcadia/FCFD-Laser/output/run_20251027_101020_Power_86.70%'
    x_init = 44_000
    dx = 10

    converted_dir = os.path.join(run_dir, "converted")
    ultra_processed_dir = os.path.join(run_dir, "ultra_processed")
    os.makedirs(ultra_processed_dir, exist_ok=True)

    converted_files = glob.glob(os.path.join(converted_dir, "*.root"))
    sorted_converted_files = sorted(
        converted_files,
        key=lambda p: int(re.search(r"run(\d+)\.root", os.path.basename(p)).group(1))
    )

    processed_files = glob.glob(os.path.join(run_dir, "processed", "*.root"))
    sorted_processed_files = sorted(
        processed_files,
        key=lambda p: int(re.search(r"run(\d+)\.root", os.path.basename(p)).group(1))
    )


    for ifile, (cpath, ppath) in enumerate(zip(sorted_converted_files, sorted_processed_files)):
        run_id = ifile
        x_um = x_init + dx * ifile
        y_um, z_um = 35000, 86500

        cols = ultra_processed_columns(
            converted_file_path=cpath,
            processed_file_path=ppath,
            analog_channels=analog_channels,
            channel_map=channel_map,
            run_id=run_id,
            x_um=x_um,
            y_um=y_um,
            z_um=z_um,
            n_events=2500,
            lp2_cols=LP2_50_COLS,
        )

        save_to_root(cols, out_dir=ultra_processed_dir, run_id=run_id, x_um=x_um, y_um=y_um, z_um=z_um)
        # break


    