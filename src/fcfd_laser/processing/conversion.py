#!/usr/bin/env python3
# fcfd_laser/processing/conversion.py
# LeCroy .trc -> ROOT writer (multi-channel, segmented, vectorized)

import os
import struct
import numpy as np
import logging
from typing import Tuple, List, Dict

try:
    import ROOT  # PyROOT
except Exception as e:
    raise RuntimeError("PyROOT is required for conversion. Ensure ROOT is available in your env.") from e


# ---- Fixed offsets in LeCroy WAVEDESC (same as your legacy) ----
WAVEDESC = 11
aCOMM_TYPE         = WAVEDESC + 32
aCOMM_ORDER        = WAVEDESC + 34
aWAVE_DESCRIPTOR   = WAVEDESC + 36
aUSER_TEXT         = WAVEDESC + 40
aTRIGTIME_ARRAY    = WAVEDESC + 48
aWAVE_ARRAY_1      = WAVEDESC + 60
aINSTRUMENT_NAME   = WAVEDESC + 76
aWAVE_ARRAY_COUNT  = WAVEDESC + 116
aPNTS_PER_SCREEN   = WAVEDESC + 120
aFIRST_VALID_PNT   = WAVEDESC + 124
aLAST_VALID_PNT    = WAVEDESC + 128
aSEGMENT_INDEX     = WAVEDESC + 140
aSUBARRAY_COUNT    = WAVEDESC + 144
aNOM_SUBARRAY_COUNT= WAVEDESC + 174
aVERTICAL_GAIN     = WAVEDESC + 156
aVERTICAL_OFFSET   = WAVEDESC + 160
aHORIZ_INTERVAL    = WAVEDESC + 176
aHORIZ_OFFSET      = WAVEDESC + 180

# ---- Helpers ----
def _read_i16(f, off) -> int:
    f.seek(off); return struct.unpack('h', f.read(2))[0]

def _read_i32(f, off) -> int:
    f.seek(off); return struct.unpack('i', f.read(4))[0]

def _read_f32(f, off) -> float:
    f.seek(off); return struct.unpack('f', f.read(4))[0]

def _read_f64(f, off) -> float:
    f.seek(off); return struct.unpack('d', f.read(8))[0]


def get_waveform_block_offsets(path: str) -> Tuple[int, int]:
    with open(path, 'rb') as f:
        user_text_len  = _read_i32(f, aUSER_TEXT)
        trigtime_len   = _read_i32(f, aTRIGTIME_ARRAY)
        wavedesc_len   = _read_i32(f, aWAVE_DESCRIPTOR)
    offset_trig  = WAVEDESC + wavedesc_len + user_text_len
    full_offset  = offset_trig + trigtime_len
    return offset_trig, full_offset


def get_configuration(path: str) -> Tuple[int, int, float, float, float, str]:
    with open(path, 'rb') as f:
        comm_type   = _read_i16(f, aCOMM_TYPE)     # 1 = 16-bit word
        comm_order  = _read_i16(f, aCOMM_ORDER)    # 0 = HIFIRST (big), 1 = LOFIRST (little)
        nsegments   = _read_i32(f, aSUBARRAY_COUNT)
        wave_count  = _read_i32(f, aWAVE_ARRAY_COUNT)
        points_per_frame = int(wave_count // max(nsegments, 1))
        horiz_dt    = _read_f32(f, aHORIZ_INTERVAL)
        vert_gain   = _read_f32(f, aVERTICAL_GAIN)
        vert_off    = _read_f32(f, aVERTICAL_OFFSET)
        f.seek(aINSTRUMENT_NAME); instrument = f.read(16).rstrip(b'\0').decode(errors='ignore')
    # We assume standard 16-bit word + little endian
    if comm_type != 1:
        raise RuntimeError(f"{os.path.basename(path)}: Unsupported COMM_TYPE={comm_type} (expected 1)")
    if comm_order not in (0, 1):
        raise RuntimeError(f"{os.path.basename(path)}: Unknown COMM_ORDER={comm_order}")
    return nsegments, points_per_frame, horiz_dt, vert_gain, vert_off, instrument


def get_segment_times_and_offsets(path: str, trig_block_offset: int, nsegments: int) -> Tuple[np.ndarray, np.ndarray]:
    times  = np.empty(nsegments, dtype=np.float64)
    offsets= np.empty(nsegments, dtype=np.float64)
    with open(path, 'rb') as f:
        f.seek(trig_block_offset)
        # Each segment contributes (double trigger_time, double horiz_offset)
        buf = f.read(nsegments * (8 + 8))
    mv = memoryview(buf)
    # vectorized unpack
    times[:]   = np.frombuffer(mv[:8*nsegments], dtype='<f8')
    offsets[:] = np.frombuffer(mv[8*nsegments:], dtype='<f8')
    return times, offsets


def _memmap_channel_i2_le(path: str, data_offset: int, nsegments: int, points_per_frame: int) -> np.memmap:
    # Entire waveform array is nsegments*points_per_frame 16-bit signed little-endian
    return np.memmap(path, dtype='<i2', mode='r',
                     offset=data_offset, shape=(nsegments * points_per_frame,))


def _extract_segment(mm: np.memmap, seg: int, pts: int) -> np.ndarray:
    a = seg * pts
    b = a + pts
    return mm[a:b]


# ---- Public entry point ----
def convert_run(raw_dir: str,
                scan_num: int,
                channels: List[int],
                out_dir: str,
                logger: logging.Logger) -> str:
    """
    Convert a run's TRC files into ROOT.
      raw_dir: directory containing C#--Trace{run}.trc
      scan_num: trace index (e.g., 1)
      channels: e.g., [1,2,3,4] (use available subset automatically)
      out_dir: where to write converted_run{run}.root
    Returns: output ROOT file path.
    """
    os.makedirs(out_dir, exist_ok=True)
    files = {ch: os.path.join(raw_dir, f"C{ch}--Trace{scan_num}.trc") for ch in channels}
    files = {ch: fp for ch, fp in files.items() if os.path.isfile(fp)}
    if not files:
        raise FileNotFoundError(f"No TRC files found for Trace{scan_num} in {raw_dir}")

    # Use the smallest channel index as the reference
    ref_ch = sorted(files.keys())[0]
    ref_fp = files[ref_ch]

    # Read core metadata
    trig_off, full_off = get_waveform_block_offsets(ref_fp)
    nseg, pts, dt, vgain_ref, voff_ref, instr = get_configuration(ref_fp)
    logger.info(f"Conversion: Trace{scan_num}  nseg={nseg} pts={pts} dt={dt:.3e}s  "
                f"channels={sorted(files.keys())}  instr={instr}")

    # Per-channel vertical scales (they can differ)
    vgain: Dict[int, float] = {}
    voff:  Dict[int, float] = {}
    for ch, fp in files.items():
        nseg_c, pts_c, dt_c, vgain_c, voff_c, _ = get_configuration(fp)
        if nseg_c != nseg or pts_c != pts:
            raise RuntimeError(f"{os.path.basename(fp)}: inconsistent segments/points vs ref channel")
        if abs(dt_c - dt) > 1e-15:
            logger.warning(f"{os.path.basename(fp)}: horiz_dt differs from ref ({dt_c} vs {dt})")
        vgain[ch] = vgain_c
        voff[ch]  = voff_c

    # Segment timing
    trig_times, horiz_offs = get_segment_times_and_offsets(ref_fp, trig_off, nseg)

    # Memory-map sample arrays for each channel
    mmap_ch: Dict[int, np.memmap] = {ch: _memmap_channel_i2_le(fp, full_off, nseg, pts)
                                     for ch, fp in files.items()}

    # Prepare ROOT
    out_path = os.path.join(out_dir, f"converted_run{scan_num}.root")
    fout = ROOT.TFile(out_path, "RECREATE")
    tree = ROOT.TTree("pulse", "pulse")

    nchan = len(files)
    # Buffers (float32 payload to save space)
    i_evt = np.zeros(1, dtype=np.uint32)
    segment_time = np.zeros(1, dtype=np.float32)
    channel = np.zeros((nchan, pts), dtype=np.float32)
    time_arr = np.zeros((1, pts), dtype=np.float32)
    time_offsets = np.zeros(nchan, dtype=np.float32)

    # Branches
    tree.Branch('i_evt', i_evt, 'i_evt/i')
    tree.Branch('segment_time', segment_time, 'segment_time/F')
    tree.Branch('channel', channel, f'channel[{nchan}][{pts}]/F')
    tree.Branch('time', time_arr, f'time[1][{pts}]/F')
    tree.Branch('timeoffsets', time_offsets, f'timeoffsets[{nchan}]/F')

    # Static time axis per segment (depends on segment-specific horizontal offset)
    base_idx = np.arange(pts, dtype=np.float32)

    # Channel order for output
    ch_order = sorted(files.keys())
    ref_idx = ch_order.index(ref_ch)

	# Per-channel horizontal_offsets for deltas (costs extra reads)
    ch_offs: Dict[int, np.ndarray] = {}
    for ch, fp in files.items():
        _, trig_block = get_waveform_block_offsets(fp)
        ch_offs[ch] = get_segment_times_and_offsets(fp, trig_off, nseg)[1]


    # Event loop (vectorized per channel; memmap slice → scale/offset)
    for s in range(nseg):
        if (s % 1000) == 0 and s > 0:
            logger.info(f"  filled {s}/{nseg} segments")
        # time axis for this segment
        time_arr[0, :] = (horiz_offs[s] + dt * base_idx).astype(np.float32)
        segment_time[0] = trig_times[s]

        # fill channels
        for oi, ch in enumerate(ch_order):
            raw_i2 = _extract_segment(mmap_ch[ch], s, pts)
            # scale: physical volts = gain * raw - offset
            channel[oi, :] = (vgain[ch] * raw_i2 - voff[ch]).astype(np.float32)

        # relative offsets vs reference channel
        for oi, ch in enumerate(ch_order):
            time_offsets[oi] = float(ch_offs[ch][s] - ch_offs[ref_ch][s])


        i_evt[0] = s
        tree.Fill()

    fout.cd()
    tree.Write()
    fout.Close()

    # cleanup mmaps
    for mm in mmap_ch.values():
        del mm

    logger.info(f"Conversion complete: {out_path}")
    return out_path
