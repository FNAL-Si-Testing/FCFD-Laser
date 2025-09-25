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

# ---- Fixed offsets in LeCroy WAVEDESC (same as legacy) ----
WAVEDESC = 11
aCOMM_TYPE          = WAVEDESC + 32
aCOMM_ORDER         = WAVEDESC + 34
aWAVE_DESCRIPTOR    = WAVEDESC + 36
aUSER_TEXT          = WAVEDESC + 40
aTRIGTIME_ARRAY     = WAVEDESC + 48
aWAVE_ARRAY_1       = WAVEDESC + 60
aINSTRUMENT_NAME    = WAVEDESC + 76
aWAVE_ARRAY_COUNT   = WAVEDESC + 116
aPNTS_PER_SCREEN    = WAVEDESC + 120
aFIRST_VALID_PNT    = WAVEDESC + 124
aLAST_VALID_PNT     = WAVEDESC + 128
aSEGMENT_INDEX      = WAVEDESC + 140
aSUBARRAY_COUNT     = WAVEDESC + 144
aNOM_SUBARRAY_COUNT = WAVEDESC + 174
aVERTICAL_GAIN      = WAVEDESC + 156
aVERTICAL_OFFSET    = WAVEDESC + 160
aHORIZ_INTERVAL     = WAVEDESC + 176
aHORIZ_OFFSET       = WAVEDESC + 180

# ---- Helpers ----
def _read_i16(f, off) -> int:
    f.seek(off); return struct.unpack('h', f.read(2))[0]

def _read_i32(f, off) -> int:
    f.seek(off); return struct.unpack('i', f.read(4))[0]

def _read_f32(f, off) -> float:
    f.seek(off); return struct.unpack('f', f.read(4))[0]

def _read_f64(f, off) -> float:
    f.seek(off); return struct.unpack('d', f.read(8))[0]

def get_waveform_block_offsets(path: str) -> Tuple[int, int, int]:
    """
    Returns:
      trig_block_offset, data_block_offset, trig_block_len_bytes
    """
    with open(path, 'rb') as f:
        user_text_len  = _read_i32(f, aUSER_TEXT)
        trigtime_len   = _read_i32(f, aTRIGTIME_ARRAY)
        wavedesc_len   = _read_i32(f, aWAVE_DESCRIPTOR)
    trig_off  = WAVEDESC + wavedesc_len + user_text_len
    data_off  = trig_off + trigtime_len
    return trig_off, data_off, trigtime_len

def get_configuration(path: str) -> Tuple[int, int, float, float, float, str, int]:
    """
    Returns:
      nsegments, points_per_frame, horiz_dt, vert_gain, vert_off, instrument, comm_order
    """
    with open(path, 'rb') as f:
        comm_type   = _read_i16(f, aCOMM_TYPE)     # 1 = 16-bit word
        comm_order  = _read_i16(f, aCOMM_ORDER)    # 0 = big-endian (HIFIRST), 1 = little-endian (LOFIRST)
        nsegments   = _read_i32(f, aSUBARRAY_COUNT)
        wave_count  = _read_i32(f, aWAVE_ARRAY_COUNT)
        points_per_frame = int(wave_count // max(nsegments, 1))
        horiz_dt    = _read_f32(f, aHORIZ_INTERVAL)
        vert_gain   = _read_f32(f, aVERTICAL_GAIN)
        vert_off    = _read_f32(f, aVERTICAL_OFFSET)
        f.seek(aINSTRUMENT_NAME)
        instrument = f.read(16).rstrip(b'\0').decode(errors='ignore')

    if comm_type != 1:
        raise RuntimeError(f"{os.path.basename(path)}: Unsupported COMM_TYPE={comm_type} (expected 1 for 16-bit).")
    if comm_order not in (0, 1):
        raise RuntimeError(f"{os.path.basename(path)}: Unknown COMM_ORDER={comm_order} (0=big,1=little).")
    return nsegments, points_per_frame, horiz_dt, vert_gain, vert_off, instrument, comm_order

def get_segment_times_and_offsets(path: str,
                                  trig_block_offset: int,
                                  nsegments: int,
                                  comm_order: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    Vectorized and correct decoding of interleaved [time, offset] doubles per segment.
    Respects COMM_ORDER for endianness.
    """
    dt = '<f8' if comm_order == 1 else '>f8'  # little vs big endian doubles
    n_doubles = 2 * nsegments
    n_bytes = n_doubles * 8
    with open(path, 'rb') as f:
        f.seek(trig_block_offset)
        buf = f.read(n_bytes)
    if len(buf) != n_bytes:
        raise RuntimeError(f"{os.path.basename(path)}: TRIGTIME_ARRAY truncated "
                           f"(got {len(buf)} bytes, expected {n_bytes}).")
    arr = np.frombuffer(buf, dtype=dt)
    if arr.size != n_doubles:
        raise RuntimeError(f"{os.path.basename(path)}: Unexpected TRIGTIME_ARRAY length.")
    arr = arr.reshape(nsegments, 2)  # [[t0,off0],[t1,off1],...]
    times   = arr[:, 0].copy()
    offsets = arr[:, 1].copy()
    return times, offsets

def _memmap_channel_i2(path: str,
                       data_offset: int,
                       nsegments: int,
                       points_per_frame: int,
                       comm_order: int) -> np.memmap:
    """
    Memmap entire waveform block as signed 16-bit with proper endianness.
    Layout is concatenated segments: nsegments*points_per_frame samples.
    """
    dt = '<i2' if comm_order == 1 else '>i2'
    return np.memmap(path, dtype=dt, mode='r', offset=data_offset,
                     shape=(nsegments * points_per_frame,))

def _extract_segment(mm: np.memmap, seg: int, pts: int) -> np.ndarray:
    a = seg * pts
    b = a + pts
    return mm[a:b]

# ---- Public entry point ----
def convert_run(raw_dir: str,
                scan_num: int,
                channels: List[int],
                out_dir: str,
                logger: logging.Logger,
                prefix: str="optimized") -> str:
    """
    Convert a run's TRC files into ROOT.
      raw_dir: directory containing C#--Trace{run}.trc
      scan_num: trace index (e.g., 1)
      channels: e.g., [1,2,3,4] (subset auto-detected)
      out_dir: where to write converted_run{run}.root
    Returns: output ROOT file path.
    """
    os.makedirs(out_dir, exist_ok=True)
    files = {ch: os.path.join(raw_dir, f"C{ch}--Trace{scan_num}.trc") for ch in channels}
    files = {ch: fp for ch, fp in files.items() if os.path.isfile(fp)}
    if not files:
        raise FileNotFoundError(f"No TRC files found for Trace{scan_num} in {raw_dir}")

    # Reference channel (smallest index)
    ref_ch = sorted(files.keys())[0]
    ref_fp = files[ref_ch]

    # Read reference metadata
    ref_trig_off, ref_data_off, ref_trig_len = get_waveform_block_offsets(ref_fp)
    nseg, pts, dt, vgain_ref, voff_ref, instr, comm_order_ref = get_configuration(ref_fp)
    logger.info(f"Conversion Trace{scan_num}: nseg={nseg} pts={pts} dt={dt:.3e}s "
                f"channels={sorted(files.keys())} instr={instr} comm_order={comm_order_ref}")

    # Per-channel configs
    vgain: Dict[int, float] = {}
    voff:  Dict[int, float] = {}
    trig_offs: Dict[int, int] = {}
    data_offs: Dict[int, int] = {}
    comm_orders: Dict[int, int] = {}

    for ch, fp in files.items():
        t_off, d_off, t_len = get_waveform_block_offsets(fp)
        nseg_c, pts_c, dt_c, vgain_c, voff_c, _, comm_order_c = get_configuration(fp)

        # Consistency checks (hard errors for shape mismatch)
        if nseg_c != nseg or pts_c != pts:
            raise RuntimeError(f"{os.path.basename(fp)}: segments/points differ from reference "
                               f"({nseg_c},{pts_c}) vs ({nseg},{pts}).")
        if abs(dt_c - dt) > 1e-15:
            logger.warning(f"{os.path.basename(fp)}: horiz_dt differs from ref ({dt_c} vs {dt}).")

        # NOTE: LeCroy usually keeps comm_order consistent across channels, but we still store per-channel to be robust.
        vgain[ch] = vgain_c
        voff[ch]  = voff_c
        trig_offs[ch] = t_off
        data_offs[ch] = d_off
        comm_orders[ch] = comm_order_c

        # Soft check on trig block size (should be 16 * nseg bytes)
        expected_trig_bytes = 16 * nseg
        if t_len != expected_trig_bytes:
            logger.warning(f"{os.path.basename(fp)}: TRIGTIME_ARRAY length {t_len} bytes, "
                           f"expected {expected_trig_bytes}. Proceeding.")

    # Segment timing from reference channel (times and offsets)
    trig_times, horiz_offs = get_segment_times_and_offsets(
        ref_fp, ref_trig_off, nseg, comm_orders[ref_ch]
    )

    # Memory-map sample arrays per channel with correct endianness
    mmap_ch: Dict[int, np.memmap] = {
        ch: _memmap_channel_i2(fp, data_offs[ch], nseg, pts, comm_orders[ch])
        for ch, fp in files.items()
    }

    # Prepare ROOT
    out_path = os.path.join(out_dir, f"{prefix}_converted_run{scan_num}.root")
    fout = ROOT.TFile(out_path, "RECREATE")
    tree = ROOT.TTree("pulse", "pulse")

    nchan = len(files)
    ch_order = sorted(files.keys())

    # Buffers
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

    # Precompute base index for time axis
    base_idx = np.arange(pts, dtype=np.float32)

    # Per-channel horizontal offsets for deltas (read ONCE, using each channel's own trig offset)
    ch_offs: Dict[int, np.ndarray] = {}
    for ch, fp in files.items():
        ch_times, ch_offsets = get_segment_times_and_offsets(
            fp, trig_offs[ch], nseg, comm_orders[ch]
        )
        # Optional: check that trigger times align within a tolerance
        if not np.allclose(ch_times, trig_times, rtol=0, atol=1e-15):
            logging.warning(f"{os.path.basename(fp)}: trigger times differ from reference.")
        ch_offs[ch] = ch_offsets

    # Event loop
    for s in range(nseg):
        if (s % 1000) == 0 and s > 0:
            logger.info(f"  filled {s}/{nseg} segments")

        # Time axis for this segment (float32 payload to keep files compact)
        time_arr[0, :] = (horiz_offs[s] + dt * base_idx).astype(np.float32)
        # Optional monotonicity check (cheap)
        if time_arr[0, 1] <= time_arr[0, 0]:
            raise RuntimeError("Non-monotonic time axis detected.")

        segment_time[0] = np.float32(trig_times[s])

        # Fill channels (vectorized scale/offset)
        for oi, ch in enumerate(ch_order):
            raw_i2 = _extract_segment(mmap_ch[ch], s, pts)
            # physical volts = gain * raw - offset
            np.subtract(np.float32(vgain[ch]) * raw_i2.astype(np.float32, copy=False),
                        np.float32(voff[ch]),
                        out=channel[oi, :])

        # Relative offsets vs reference
        ref_ch_off = ch_offs[ref_ch][s]
        for oi, ch in enumerate(ch_order):
            time_offsets[oi] = np.float32(ch_offs[ch][s] - ref_ch_off)

        i_evt[0] = s
        tree.Fill()

    fout.cd()
    tree.Write()
    fout.Close()

    # Cleanup mmaps
    for mm in mmap_ch.values():
        del mm

    logger.info(f"Conversion complete: {out_path}")
    return out_path

if __name__ == "__main__":
    import argparse, logging, time

    parser = argparse.ArgumentParser(description="LeCroy .trc → ROOT converter (modular version)")
    parser.add_argument("--rawDir", type=str, default="output/DEBUG_RUN/raw",
                        help="Directory containing TRC files")
    parser.add_argument("--scanNum", type=int, required=True,
                        help="Trace/run number to process (e.g., 1 if files are C1--Trace1.trc, C2--Trace1.trc, ...)")
    parser.add_argument("--channels", type=int, nargs="+", default=None,
                        help="Channel numbers to process (e.g. 1 2 3). Default = auto-detect")
    parser.add_argument("--outDir", type=str, default="output/DEBUG_RUN/converted",
                        help="Output directory for ROOT file")
    parser.add_argument("-p", "--prefix", type=str, default="optimized",
                        help="Prefix tag for output file naming")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    # Logging
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="[%(levelname)s] %(message)s")
    logger = logging.getLogger("converter")

    # If channels not given, auto-detect from files
    if args.channels is None:
        files = [f for f in os.listdir(args.rawDir) if f.startswith("C") and f"--Trace{args.scanNum}" in f]
        chs = []
        for f in files:
            try:
                # Extract channel number from "C#--TraceN.trc"
                ch_num = int(f.split("--")[0][1:]) # "C7--Trace1.trc" -> 7
                chs.append(ch_num)
            except Exception:
                continue
        args.channels = sorted(chs)
        if not args.channels:
            raise FileNotFoundError(f"No TRC files found in {args.rawDir} for Trace{args.scanNum}")

    # Run conversion
    start = time.time()
    out_file = convert_run(args.rawDir, args.scanNum, args.channels, args.outDir, logger, prefix=args.prefix)
    end = time.time()

    elapsed = end - start
    logger.info(f"[CLI] Wrote ROOT file: {out_file}")
    logger.info(f"[CLI] Conversion took {elapsed:.2f} seconds.")