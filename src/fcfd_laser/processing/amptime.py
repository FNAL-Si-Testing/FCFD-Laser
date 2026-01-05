# src


import uproot as ur
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
import re
from tqdm import tqdm
from iminuit import Minuit
from iminuit.cost import LeastSquares 
from scipy.stats import norm


def get_amp_time(path, iCh_ana_idx, iEvent, ch_map):
    assert iCh_ana_idx in ch_map, f"Analog channel index {iCh_ana_idx} not in map!"
    with ur.open(path) as data_file:
        data = data_file["pulse"]
        channel_data = data.arrays(["channel"], library="np")
        time_data = data.arrays(["time"], library="np")
        ch_arr = channel_data["channel"]  
        time_arr = time_data["time"][iEvent, 0, :] * 1e9

        iCh_dig_idx = ch_map[iCh_ana_idx]
        y_ana = ch_arr[iEvent, iCh_ana_idx, :]
        y_dig = ch_arr[iEvent, iCh_dig_idx, :]

        rise_idx, fall_idx, _, _ = find_pulse_edges(y_dig)
        
        return rise_idx, fall_idx, time_arr, y_ana, y_dig
    
def find_pulse_edges(ch, noise_window=100, noise_factor=5, peak_fraction=0.2):
    dy_dt = np.gradient(ch.astype(np.float64, copy=False), edge_order=2)

    baseline = dy_dt[:noise_window]
    sigma = np.std(baseline)

    thr_noise = noise_factor * sigma
    thr_peak  = peak_fraction * np.max(np.abs(dy_dt))

    thr_pos = max(thr_noise, thr_peak)     
    thr_neg = -max(thr_noise, thr_peak)    

    rise_candidates = np.where(dy_dt > thr_pos)[0]
    fall_candidates = np.where(dy_dt < thr_neg)[0]

    rise_idx, fall_idx = None, None
    if rise_candidates.size > 0:
        rise_idx = rise_candidates[0]  
    if fall_candidates.size > 0:
        # TZ: I changed this to take the first fall candidate
        # I think that we need to do this in the case we have
        # a double pulse, we only care about the first one
        fall_idx = fall_candidates[0]

    return rise_idx, fall_idx, thr_pos, thr_neg

def process_file_matrix(path, analog_channels, ch_map, n_events=1000, offset=0):
    """
    Parse one ROOT file and return amplitudes as a dense matrix:
        shape = (n_events_in_file, n_channels)
    Values are NaN where a channel/event failed edge detection.
    """
    with ur.open(path) as f:
        arr = f["pulse"].arrays(["channel"], library="np")["channel"]  # (events, channels, samples)

    n_ev = min(n_events, arr.shape[0])
    n_ch = len(analog_channels)
    amps = np.full((n_ev, n_ch), np.nan, dtype=np.float64)

    for iEvent in range(n_ev):
        for j, ana_idx in enumerate(analog_channels):
            dig_idx = ch_map[ana_idx]

            y_ana = arr[iEvent, ana_idx, :]
            y_dig = arr[iEvent, dig_idx, :]

            # rise_idx, fall_idx, _, _ = find_pulse_edges(y_dig)
            rise_idx, fall_idx = 3301, 3701
            # if rise_idx is None or fall_idx is None or fall_idx <= rise_idx:
            # if rise_idx is None or fall_idx is None or fall_idx-offset <= rise_idx+offset:
            #     continue
                # amps[iEvent, j] = np.nan
            amps[iEvent, j] = y_ana[rise_idx+offset:fall_idx-offset].mean()

    return amps  # (n_ev, n_ch)

def process_amp_fast(path, analog_channels, rise_idx=3301, fall_idx=3701):
    with ur.open(path) as f:
        arr = f["pulse"].arrays(["channel"], library="np")["channel"]  # (events, channels, samples)
    return arr[:,analog_channels,rise_idx:fall_idx].mean(axis=2)

def build_amp_cube(sorted_converted_files, analog_channels, ch_map, n_events=1000):
    """
    Build a 3D cube across files:
        cube.shape = (n_files, n_events, n_channels)
    Files with fewer events get NaN-padded to n_events.
    """
    n_files = len(sorted_converted_files)
    n_ch = len(analog_channels)
    cube = np.full((n_files, n_events, n_ch), np.nan, dtype=np.float64)

    for f_idx, scf in enumerate(tqdm(sorted_converted_files, desc="Files")):
        M = process_file_matrix(scf, analog_channels, ch_map, n_events=n_events)  # (n_ev_in_file, n_ch)
        n_ev = M.shape[0]
        cube[f_idx, :n_ev, :] = M

    return cube  # (n_files, n_events, n_channels)

def get_processed_data(idx, run_dir='/home/arcadia/FCFD-Laser/output/run_20251002_160916_Power_80.0%'):
    processed_path = os.path.join(run_dir, "processed")
    processed_files = glob.glob(os.path.join(processed_path, "*.root"))
    if not processed_files:
        raise FileNotFoundError(f"No .root files found in {processed_path}")
    sorted_files = sorted(
        processed_files,
        key=lambda path: int(re.search(r'run(\d+)\.root', path).group(1))
    )
    data_path = sorted_files[idx]
    with ur.open(data_path) as data_file:
        amp_data = data_file["pulse;1"]['amp'].arrays(library="np")['amp']
        baseline_data = data_file["pulse;1"]['baseline'].arrays(library="np")['baseline']
        LP2_50_data = data_file["pulse;1"]['LP2_50'].arrays(library="np")['LP2_50'] * 1e9
        return amp_data, baseline_data, LP2_50_data
    
def clean(x, window=[45, 48]):
    mask = np.isfinite(x)
    mask = mask & (x > 1e-3)
    mask = mask & (x > window[0])
    mask = mask & (x < window[1])
    return x[mask]

def fit_binned_gaussian(data, bins=100):
    counts, edges = np.histogram(data, bins=bins)
    bin_centers = (edges[:-1] + edges[1:]) / 2
    
    y_err = np.sqrt(counts)
    y_err[y_err == 0] = 1

    def unscaled_gauss(x, A, mu, sigma):
        """A Gaussian function with an amplitude parameter 'A'."""
        return A * np.exp(-0.5 * ((x - mu) / sigma)**2)

    cost = LeastSquares(bin_centers, counts, y_err, unscaled_gauss)

    m = Minuit(cost, A=np.max(counts), mu=np.mean(data), sigma=np.std(data))
    m.limits["sigma"] = (1e-6, None) 
    m.limits["A"] = (0, None)       
    m.migrad()
    m.hesse()
    return m

def plot_with_binned_fit(data, fit_result, strip_label, color='C0', bins=100):
    A = fit_result.values["A"]
    mu = fit_result.values["mu"]
    sigma = fit_result.values["sigma"]
    A_err = fit_result.errors["A"]
    mu_err = fit_result.errors["mu"]
    sigma_err = fit_result.errors["sigma"]

    fig, ax = plt.subplots(figsize=(6, 4))
    counts, edges, _ = ax.hist(data, bins=bins, histtype="step", label=f"{strip_label} Δt", color=color)

    xx = np.linspace(edges[0], edges[-1], 800)
    fitted_curve = A * np.exp(-0.5 * ((xx - mu) / sigma)**2)

    fit_label = (rf"Fit: $A={A:.1f}\pm{A_err:.1g}, \mu={mu:.4f}\pm{mu_err:.2g}, \sigma={sigma:.4f}\pm{sigma_err:.2g}$")
    ax.plot(xx, fitted_curve, color='k', lw=2, label=fit_label)
    
    ax.set_xlabel(r"$\Delta$ t = t(strip) - t(laser) [ns]")
    ax.set_ylabel("Counts / bin")
    ax.legend()
    ax.set_title(f"{strip_label} Timing Fit (Binned)")
    plt.tight_layout()
    plt.show()
