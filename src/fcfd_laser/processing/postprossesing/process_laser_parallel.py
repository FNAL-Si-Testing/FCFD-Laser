import sys
sys.path.insert(1, '..')
import utils

import uproot as ur
import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial

laser_scan_path = Path("/Users/dzenger/FCFD/data/Board_3/board_3_update_20260217/run_20260217_105905_Power_85p5_power_test")
n_files = 10
n_events = 1000

column_float_names = ["x_dut", "y_dut", "z_dut",
                "amp_0", "amp_1", "amp_2", "amp_3", "amp_4", "amp_5", "amp_6", "amp_7",
               "baseline_0", "baseline_1", "baseline_2", "baseline_3", "baseline_4", "baseline_5", "baseline_6", "baseline_7",
               "t_0", "t_1", "t_2", "t_3", "t_4", "t_5", "t_6", "t_7",
               "analog_strip_1", "analog_strip_2", "analog_strip_3"]
column_bool_names = ["analog_disc_1", "analog_disc_2", "analog_disc_3"]
column_int_names = ["file", "idx"]


def process_single_file(i, laser_scan_path, n_events):
    """
    Process a single file and return a DataFrame with all events from that file.
    This function will be called in parallel for each file.
    """
    # Read amplitude, baseline, and timing data
    with ur.open(laser_scan_path / f"processed/processed_legacy_converted_run{i}.root") as fdata:
        amp_data = fdata["pulse"]["amp"].array(library="np")
        baseline_data = fdata["pulse"]["baseline"].array(library="np")
        timing = fdata["pulse"]["LP2_50"].array(library="np")
    
    # Read channel data
    with ur.open(laser_scan_path / f"converted/legacy_converted_run{i}.root") as f:
        d = f["pulse"]["channel"].array(library="np")

    # Read position data
    name_base = f"run_{i:04d}"
    file_name = list((laser_scan_path / "ultra_processed").glob(f"{name_base}_*.root"))[0]
    
    with ur.open(file_name) as f:
        x_dut = f["pulse_summary"]["x_um"].array(library="np")[0]
        y_dut = f["pulse_summary"]["y_um"].array(library="np")[0]
        z_dut = f["pulse_summary"]["z_um"].array(library="np")[0]
    
    # Initialize arrays for this file's data
    file_data = {
        'file': np.full(n_events, i, dtype=np.int32),
        'idx': np.arange(n_events, dtype=np.int32),
        'x_dut': np.full(n_events, x_dut, dtype=np.float64),
        'y_dut': np.full(n_events, y_dut, dtype=np.float64),
        'z_dut': np.full(n_events, z_dut, dtype=np.float64),
    }
    
    # Add amp, baseline, and timing columns
    for col_idx in range(8):
        file_data[f'amp_{col_idx}'] = amp_data[:, col_idx].astype(np.float64)
        file_data[f'baseline_{col_idx}'] = baseline_data[:, col_idx].astype(np.float64)
        file_data[f't_{col_idx}'] = timing[:, col_idx].astype(np.float64)
    
    # Initialize analog strip and disc arrays
    analog_strip_1 = np.full(n_events, np.nan, dtype=np.float64)
    analog_strip_2 = np.full(n_events, np.nan, dtype=np.float64)
    analog_strip_3 = np.full(n_events, np.nan, dtype=np.float64)
    analog_disc_1 = np.full(n_events, False, dtype=bool)
    analog_disc_2 = np.full(n_events, False, dtype=bool)
    analog_disc_3 = np.full(n_events, False, dtype=bool)
    
    # Process each event - this is still sequential but within each file
    # This could be further parallelized if needed
    for ix in range(n_events):
        # Process analog strip 1
        e_1 = utils.get_edges(d[ix, 4, :])
        a_1, a_d_1, e_1_0, e_1_1 = utils.get_amplitude(
            d[ix, 1, :], e_1, rise_start=2000, default_edge=(3301, 3701), min_width=501
        )
        analog_strip_1[ix] = a_1 * 1000
        analog_disc_1[ix] = not a_d_1
        
        # Process analog strip 2
        e_2 = utils.get_edges(d[ix, 5, :])
        a_2, a_d_2, e_2_0, e_2_1 = utils.get_amplitude(
            d[ix, 2, :], e_2, rise_start=2000, default_edge=(3301, 3701), min_width=501
        )
        analog_strip_2[ix] = a_2 * 1000
        analog_disc_2[ix] = not a_d_2

        # Process analog strip 3
        e_3 = utils.get_edges(d[ix, 6, :])
        a_3, a_d_3, e_3_0, e_3_1 = utils.get_amplitude(
            d[ix, 3, :], e_3, rise_start=2000, default_edge=(3301, 3701), min_width=501
        )
        analog_strip_3[ix] = a_3 * 1000
        analog_disc_3[ix] = not a_d_3
    
    # Add analog data to file_data dict
    file_data['analog_strip_1'] = analog_strip_1
    file_data['analog_strip_2'] = analog_strip_2
    file_data['analog_strip_3'] = analog_strip_3
    file_data['analog_disc_1'] = analog_disc_1
    file_data['analog_disc_2'] = analog_disc_2
    file_data['analog_disc_3'] = analog_disc_3
    
    # Create DataFrame for this file
    df = pd.DataFrame(file_data)
    
    return df


if __name__ == '__main__':
    # Determine number of processes to use (leave 1 core free)
    n_processes = max(1, cpu_count() - 1)
    print(f"Using {n_processes} parallel processes")
    
    # Create partial function with fixed arguments
    process_func = partial(process_single_file, 
                          laser_scan_path=laser_scan_path, 
                          n_events=n_events)
    
    # Process files in parallel
    file_indices = range(1, n_files + 1)
    
    with Pool(processes=n_processes) as pool:
        # Use imap for progress tracking
        results = list(tqdm(
            pool.imap(process_func, file_indices),
            total=n_files,
            desc="Processing files"
        ))
    
    # Combine all DataFrames
    print("Combining results...")
    df_laser = pd.concat(results, ignore_index=True)
    
    # Sort by file and idx to match original ordering
    df_laser = df_laser.sort_values(['file', 'idx']).reset_index(drop=True)
    
    # Save outputs
    output_base = "/Users/dzenger/FCFD/data/Board_3/board_3_update_20260217/near_double"
    
    print("Saving parquet...")
    df_laser.to_parquet(f"{output_base}.parquet")
    
    # print("Saving CSV...")
    # df_laser.to_csv(f"{output_base}.csv")
    
    # print("Saving HDF5...")
    # df_laser.to_hdf(f"{output_base}.h5", key="event")
    
    print("Done!")
