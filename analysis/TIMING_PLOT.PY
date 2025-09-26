# run_robust_timing_analysis.py

import ROOT
import uproot as ur
import numpy as np
import os
import glob
import re

def setup_paths():
    """Defines and creates the necessary directory paths."""
    project_root = os.getcwd()
    output_dir = os.path.join(project_root, "output")
    fig_dir = os.path.join(project_root, "figs")
    os.makedirs(fig_dir, exist_ok=True)
    return output_dir, fig_dir

def get_latest_run_dir(power, base_path):
    """Finds the most recent run directory for a given power setting."""
    pattern = os.path.join(base_path, f"run_*_Power_{power}%")
    run_dirs = sorted(glob.glob(pattern))
    if not run_dirs:
        raise FileNotFoundError(f"No run directory found for Power={power}% in {base_path}")
    print(f"Found latest run directory: {run_dirs[-1]}")
    return run_dirs[-1]

def get_processed_data(power, idx, base_path):
    """Loads the LP2_50 data from a specific processed ROOT file."""
    run_dir = get_latest_run_dir(power, base_path)
    processed_path = os.path.join(run_dir, "processed")
    processed_files = glob.glob(os.path.join(processed_path, "*.root"))
    if not processed_files:
        raise FileNotFoundError(f"No .root files found in {processed_path}")
    
    sorted_files = sorted(
        processed_files,
        key=lambda path: int(re.search(r'run(\d+)\.root', path).group(1))
    )
    
    data_path = sorted_files[idx]
    print(f"Loading data from: {data_path}")
    with ur.open(data_path) as data_file:
        lp2_50_data = data_file["pulse;1"]["LP2_50"].arrays(library="np")["LP2_50"] * 1e9
    return lp2_50_data

def clean_data(x):
    """Applies cleaning cuts to the time difference data."""
    mask = np.isfinite(x)
    mask &= (x > 1e-3)
    mask &= (x < 48)
    return x[mask]

def create_and_fit_plot(data_array, strip_name, color, power, fig_dir):
    """
    Creates, robustly fits, and saves a histogram for a single strip
    using a two-step iterative fitting method.
    """
    print(f"\n--- Processing {strip_name} ---")
    
    canvas_name = f"c_{strip_name.replace(' ', '_')}"
    hist_name = f"h_{strip_name.replace(' ', '_')}"
    canvas = ROOT.TCanvas(canvas_name, f"Time Difference for {strip_name}", 800, 600)
    
    x_min, x_max = data_array.min(), data_array.max()
    hist = ROOT.TH1F(hist_name, f"Time Difference for {strip_name} (Power={power}%); #Delta t [ns]; Counts", 100, x_min, x_max)

    for val in data_array:
        hist.Fill(val)

    # --- Robust Fitting Logic ---
    # 1. Perform a "quiet" preliminary fit over the full range to get an estimate.
    #    The "Q" option suppresses the print output for this step.
    prelim_fit_func = ROOT.TF1("prelim_fit", "gaus", x_min, x_max)
    hist.Fit(prelim_fit_func, "Q")

    # 2. Get the parameters from the initial fit.
    mu = prelim_fit_func.GetParameter(1)
    sigma = prelim_fit_func.GetParameter(2)

    # 3. Define a narrower fit range (e.g., +/- 2 sigma) to exclude outliers.
    fit_range_low = mu - 2 * sigma
    fit_range_high = mu + 2 * sigma

    # 4. Perform the final, more accurate fit within the refined range.
    #    "R" uses the specified range. "L" uses a more robust Log-Likelihood method.
    final_fit_func = ROOT.TF1("final_fit", "gaus", fit_range_low, fit_range_high)
    final_fit_func.SetLineColor(color)
    hist.Fit(final_fit_func, "RL") # "R" for range, "L" for Log-Likelihood

    # --- Drawing and Saving ---
    hist.SetLineColor(color)
    hist.Draw()

    fig_path = os.path.join(fig_dir, f"{power}__{strip_name.replace(' ', '_')}_robust_fit.png")
    canvas.SaveAs(fig_path)
    print(f"Plot saved successfully to: {fig_path}")

    # Return the final fit function for the summary printout
    return hist.GetFunction("final_fit")


def main():
    """Main function to run the full analysis chain."""
    ROOT.gROOT.SetBatch(True)
    ROOT.gStyle.SetOptFit(1111)
    
    OUTPUT_DIR, FIG_DIR = setup_paths()
    
    power = float(input("Enter the power setting (e.g., 80): "))
    file_index = int(input("Enter the file index (e.g., 0): "))
    
    print("--- Starting Data Processing ---")
    data_file = get_processed_data(power, file_index, OUTPUT_DIR)
    
    laser_lp2_50 = data_file[:, 0]
    strip1_lp2_50 = data_file[:, 1]
    strip2_lp2_50 = data_file[:, 3]
    strip3_lp2_50 = data_file[:, 5]
    
    dt1 = clean_data(strip1_lp2_50 - laser_lp2_50)
    dt2 = clean_data(strip2_lp2_50 - laser_lp2_50)
    dt3 = clean_data(strip3_lp2_50 - laser_lp2_50)
    print("Data processing complete.")

    # --- Create, Fit, and Save a plot for each strip ---
    fit1 = create_and_fit_plot(dt1, "Strip 1", ROOT.kBlue, power, FIG_DIR)
    fit2 = create_and_fit_plot(dt2, "Strip 2", ROOT.kRed, power, FIG_DIR)
    fit3 = create_and_fit_plot(dt3, "Strip 3", ROOT.kGreen, power, FIG_DIR)
    
    # --- Print Final Summary of Fit Results ---
    print("\n--- Summary of Robust Fit Parameters ---")
    print(f"Strip 1: μ = {fit1.GetParameter(1):.3g} ns, σ = {abs(fit1.GetParameter(2)):.3g} ns")
    print(f"Strip 2: μ = {fit2.GetParameter(1):.3g} ns, σ = {abs(fit2.GetParameter(2)):.3g} ns")
    print(f"Strip 3: μ = {fit3.GetParameter(1):.3g} ns, σ = {abs(fit3.GetParameter(2)):.3g} ns")

if __name__ == "__main__":
    main()