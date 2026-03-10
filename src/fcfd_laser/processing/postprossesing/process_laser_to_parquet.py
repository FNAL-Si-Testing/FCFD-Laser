import edge_time_finder as utils

import uproot as ur
import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm

laser_scan_path = Path("/Users/dzenger/FCFD/data/Board_3/Board_3_Laser_Scans_20260203/Full_Scan_Laser_87p9")
n_files = 460
n_events = 1000


column_float_names = [ "x_dut", "y_dut", "z_dut",
                "amp_0", "amp_1", "amp_2", "amp_3", "amp_4", "amp_5", "amp_6", "amp_7",
               "baseline_0", "baseline_1", "baseline_2", "baseline_3", "baseline_4", "baseline_5", "baseline_6", "baseline_7",
               "t_0", "t_1", "t_2", "t_3", "t_4", "t_5", "t_6", "t_7",
               "analog_strip_1", "analog_strip_2", "analog_strip_3"]
column_bool_names = ["analog_disc_1", "analog_disc_2", "analog_disc_3"]
column_int_names = ["file", "idx"]

column_dtypes = [np.int32]*len(column_int_names) + \
    [np.float64]*len(column_float_names) + \
    [np.bool]* len(column_bool_names)

df_laser = pd.DataFrame(np.full(shape=(n_files*n_events, len(column_dtypes)), fill_value=np.nan),
                        columns=column_int_names+column_float_names+column_bool_names)

df_laser["file"] = np.repeat(np.arange(n_files),n_events).astype(np.int32) + 1
df_laser["idx"] = np.repeat(np.arange(n_events),n_files).reshape(n_files,n_events,order='F').ravel().astype(np.int32)

for i in tqdm.tqdm(range(1, n_files+1)):
    # print(f"Processing Step {i}")
    with ur.open(laser_scan_path / f"processed/processed_legacy_converted_run{i}.root") as fdata:
        amp_data = fdata["pulse"]["amp"].array(library="np")
        baseline_data = fdata["pulse"]["baseline"].array(library="np")
        timing = fdata["pulse"]["LP2_50"].array(library="np")
    
    with ur.open(laser_scan_path / f"converted/legacy_converted_run{i}.root") as f:
        d = f["pulse"]["channel"].array(library="np")

    name_base = f"run_{i:04d}"

    file_name = list((laser_scan_path / "ultra_processed").glob(f"{name_base}_*.root"))[0]
    
    with ur.open(file_name) as f:
        x_dut = f["pulse_summary"]["x_um"].array(library="np")[0]
        y_dut = f["pulse_summary"]["y_um"].array(library="np")[0]
        z_dut = f["pulse_summary"]["z_um"].array(library="np")[0]
    
    df_sel = df_laser["file"] == i

    df_laser.loc[df_sel, ["amp_0", "amp_1", "amp_2", "amp_3", "amp_4", "amp_5", "amp_6", "amp_7"]] = amp_data
    df_laser.loc[df_sel, ["baseline_0", "baseline_1", "baseline_2", "baseline_3", "baseline_4", "baseline_5", "baseline_6", "baseline_7"]] = baseline_data
    df_laser.loc[df_sel, ["t_0", "t_1", "t_2", "t_3", "t_4", "t_5", "t_6", "t_7"]] = timing
    df_laser.loc[df_sel, "x_dut"] = x_dut
    df_laser.loc[df_sel, "y_dut"] = y_dut
    df_laser.loc[df_sel, "z_dut"] = z_dut
    
    for j, r in tqdm.tqdm(df_laser.loc[df_laser["file"] == i].iterrows(), leave=False):
        ix = int(r["idx"])
        e_1 = utils.get_edges(d[ix,4,:])
        a_1, a_d_1, e_1_0, e_1_1 = utils.get_amplitude(d[ix,1,:], e_1, rise_start=2000, default_edge=(3301,3701), min_width=501)
        df_laser.loc[j, "analog_strip_1"] = a_1*1000
        df_laser.loc[j, "analog_disc_1"] = not a_d_1
        
        e_2 = utils.get_edges(d[ix,5,:])
        a_2, a_d_2, e_2_0, e_2_1 = utils.get_amplitude(d[ix,2,:], e_2, rise_start=2000, default_edge=(3301,3701), min_width=501)
        df_laser.loc[j, "analog_strip_2"] = a_2*1000
        df_laser.loc[j, "analog_disc_2"] = not a_d_2

        e_3 = utils.get_edges(d[ix,6,:])
        a_3, a_d_3, e_3_0, e_3_1 = utils.get_amplitude(d[ix,3,:], e_3, rise_start=2000, default_edge=(3301,3701), min_width=501)
        df_laser.loc[j, "analog_strip_3"] = a_3*1000
        df_laser.loc[j, "analog_disc_3"] = not a_d_3

df_laser.to_parquet("/Users/dzenger/FCFD/data/Board_3/Board_3_Laser_Scans_20260203/data_laser_87p9_board_3.parquet")