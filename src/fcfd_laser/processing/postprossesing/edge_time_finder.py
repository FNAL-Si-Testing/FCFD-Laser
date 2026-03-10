from collections import deque
import numpy as np
from scipy.signal import find_peaks
import matplotlib.pyplot as plt
import uproot as ur

# Discriminator + Amplitude Analysis Utils

def get_edges(pulse, height=0.04, **kwargs):

    edges = []

    # First find where the absolute value of gradient is above a value
    pulse_grad = np.gradient(pulse)
    rising_candidates = deque(find_peaks(pulse_grad, height=height, **kwargs)[0])
    falling_candidates = deque(find_peaks(-pulse_grad, height=height, **kwargs)[0])


    # No rising candidates are found
    if not rising_candidates:

        # No edges found
        if not falling_candidates:
            return [[None, None]]
        
        # Only end of one is found
        return [[None, falling_candidates.popleft()]]
    
    while rising_candidates:

        point = [rising_candidates.popleft()]

        if falling_candidates:

            fall_cand = falling_candidates.popleft()
            if fall_cand < point[0]:
                edges.append([None, fall_cand])
            
                if falling_candidates:

                    point.append(falling_candidates.popleft())
            
                else:
                    point.append(None)
            else:
                point.append(fall_cand)

            edges.append(point)
        
        else:
            point.append(None)
            edges.append(point)

    return edges


def get_edge_candidates(edges, rise_start)->list:
    edge_cands = []
    for pair in edges:
        if pair[0] is None:
            pair[0] = 0
        if pair[0] > rise_start:
            edge_cands.append(pair)
    
    return edge_cands


def get_amplitude(pulse, edges: list[list], 
                  algorithim="mean",
                  rise_start=None,
                  default_edge=(3100,4000), 
                  offset=250,
                  min_width=None):
    
    edge = None
    use_default = False

    # If there are no edges given (or found previously),
    # provide the default edges
    if (edges[0][0] is None) and (edges[0][1] is None):
        edge = default_edge
        use_default = True

    else:
        # Find the prime candidate of edges

        # First, if rise_start is given, then remove pairs that do not conform to this
        if rise_start is not None:
            edge_cands = get_edge_candidates(edges, rise_start)

            if not edge_cands:
                edge = default_edge
                use_default = True
        
        else:
            edge_cands = edges
    
    # Now, get the edge candidate
    if not use_default:

        # If we don't specify a minimum width, just get the first
        # valid candidate
        if min_width is None:
            edge = edge_cands[0]
            if edge[0] == None:
                edge[0] = 0
            if edge[1] == None:
                edge[1] = pulse.shape[0]
    
        else:

            # Keep popping until we get a valid candidtate
            while edge_cands:
                edge_cand = edge_cands.pop(0)

                # None values should be translated to actual numbers
                if edge_cand[0] == None:
                    edge_cand[0] = 0
                if edge_cand[1] == None:
                    edge_cand[1] = pulse.shape[0]

                # if the offset is too large, then we won't properly fit
                if (edge_cand[1] - edge_cand[0]) < 2*offset:
                    continue

                # If the width is too small, then move on
                if (edge_cand[1] - edge_cand[0]) < min_width:
                    continue

                edge = edge_cand
                break
    
    if edge is None:
        edge = default_edge
        use_default = True
    
    if use_default:
        return pulse[edge[0]:edge[1]].mean(), use_default, edge[0], edge[1]
    else: 
        return pulse[edge[0]+offset:edge[1]-offset].mean(), use_default, edge[0]+offset, edge[1]-offset
    
    
def get_amplitude_simple(pulse, bounds=(3301,3701)):

    return pulse[bounds[0]:bounds[1]].mean()


def process_file(convert_file_path, daq_file_path, idxs, default_bounds=(3301, 3701)):

    with ur.open(convert_file_path) as cf:
        pulses = cf["pulse"]["channel"].array(library="np")
    
    with ur.open(daq_file_path) as df:
        amp_array = np.array(df["pulse"]["amp"])
        baseline_array = np.array(df["pulse"]["baseline"])
        lp2_50 = np.array(df["pulse"]["LP2_50"])
        lp2_20 = np.array(df["pulse"]["LP2_20"])
        lp2_30 = np.array(df["pulse"]["LP2_30"])
        lp2_60 = np.array(df["pulse"]["LP2_60"])
        lp2_70 = np.array(df["pulse"]["LP2_70"])
    
    amp_results = []
    for i in idxs:
        amp_result_i = []
        for j in range(3):
            edge_candidates = get_edges(pulses[i,2*j,:])
            amp_result = get_amplitude(pulses[i, (2*j)+1, :], edge_candidates, default_bounds=default_bounds)
            amp_result_i.append(amp_result)
        amp_results.append(amp_result_i)

    return amp_results, amp_array[idxs, :, :], baseline_array[idxs, :, :], lp2_20[idxs, :, :], \
            lp2_30[idxs, :, :], lp2_50[idxs, :, :], lp2_60[idxs, :, :], lp2_70[idxs, :, :]

# Plot Utils

def plot_pulse(pulse, time, idx, channels=(0,1,2,3), pulse_slice=(None, None)):
    fig, axs = plt.subplots(1,1, figsize=(8,5))

    for c in channels:
        axs.plot(time[pulse_slice[0]:pulse_slice[1]], pulse[idx, c, pulse_slice[0]:pulse_slice[1]], label=f"Channel {c}")
    
    axs.set_xlabel("Time [s]")
    axs.set_ylabel("Voltage [V]")

    return fig, axs

