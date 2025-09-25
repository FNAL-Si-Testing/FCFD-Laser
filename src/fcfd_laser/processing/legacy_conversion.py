import struct  #struct unpack result - tuple
import numpy as np
# from ROOT import *
import ROOT
import time
import optparse
import argparse
import os
import sys

NCHAN_DEFAULT = 7

#### Memory addresses #####
WAVEDESC=11
aTEMPLATE_NAME		= WAVEDESC+ 16;
aCOMM_TYPE			= WAVEDESC+ 32;
aCOMM_ORDER			= WAVEDESC+ 34;
aWAVE_DESCRIPTOR	= WAVEDESC+ 36;	# length of the descriptor block
aUSER_TEXT			= WAVEDESC+ 40;	# length of the usertext block
aTRIGTIME_ARRAY     = WAVEDESC+ 48;
aWAVE_ARRAY_1		= WAVEDESC+ 60;	# length (in Byte) of the sample array
aINSTRUMENT_NAME	= WAVEDESC+ 76;
aINSTRUMENT_NUMBER  = WAVEDESC+ 92;
aTRACE_LABEL		= WAVEDESC+ 96;
aWAVE_ARRAY_COUNT	= WAVEDESC+ 116;
aPNTS_PER_SECREEN = WAVEDESC+120
aFIRST_VALID_PNT = WAVEDESC+124
aLAST_VALID_PNT = WAVEDESC+128
aSEGMENT_INDEX = WAVEDESC+140;
aSUBARRAY_COUNT = WAVEDESC+144
aNOM_SUBARRAY_COUNT = WAVEDESC+174
aVERTICAL_GAIN		= WAVEDESC+ 156;
aVERTICAL_OFFSET	= WAVEDESC+ 160;
aNOMINAL_BITS		= WAVEDESC+ 172;
aHORIZ_INTERVAL     = WAVEDESC+ 176;
aHORIZ_OFFSET		= WAVEDESC+ 180;
aVERTUNIT			= WAVEDESC+ 196;
aHORUNIT			= WAVEDESC+ 244;
aTRIGGER_TIME		= WAVEDESC+ 296;
aACQ_DURATION = WAVEDESC+312;
aRECORD_TYPE		= WAVEDESC+ 316;
aPROCESSING_DONE	= WAVEDESC+ 318;
aTIMEBASE			= WAVEDESC+ 324;
aVERT_COUPLING		= WAVEDESC+ 326;
aPROBE_ATT			= WAVEDESC+ 328;
aFIXED_VERT_GAIN	= WAVEDESC+ 332;
aBANDWIDTH_LIMIT	= WAVEDESC+ 334;
aVERTICAL_VERNIER	= WAVEDESC+ 336;
aACQ_VERT_OFFSET	= WAVEDESC+ 340;
aWAVE_SOURCE		= WAVEDESC+ 344;



def dump_info(filepath_in, index_in,n_points):
	x_axis = []
	y_axis = []

	# read from file
	my_index = index_in
	# start = time.time()
	my_file = open(filepath_in, 'rb')
	#WAVEDESC = my_file.read(50).find("WAVEDESC")
	#WAVEDESC = 11
	#print WAVEDESC
	my_file.seek(aCOMM_ORDER)
	comm_order = struct.unpack('h',my_file.read(2))
	print("Comm order",comm_order)
	my_file.seek(aCOMM_TYPE)
	comm_type = struct.unpack('h',my_file.read(2))
	print("Comm type",comm_type)

	my_file.seek(WAVEDESC+16)
	template_name= my_file.read(16)
	print(template_name)
	my_file.seek(WAVEDESC+76)
	# instrument_name = struct.unpack('s',my_file.read(16))
	instrument_name = my_file.read(16)
	print(instrument_name)

	my_file.seek(aWAVE_SOURCE)
	print("Wave source index is ",struct.unpack('h',my_file.read(2)))
	my_file.seek(aVERT_COUPLING)
	print("Vert coupling index is ",struct.unpack('h',my_file.read(2)))
	my_file.seek(aBANDWIDTH_LIMIT)
	print("Bandwith limiting index is ",struct.unpack('h',my_file.read(2)))
	my_file.seek(aRECORD_TYPE)
	print("Record type index is ",struct.unpack('h',my_file.read(2)))
	my_file.seek(aVERTICAL_GAIN)
	vertical_gain = struct.unpack('f',my_file.read(4))[0]
	print("Vertical gain is ",vertical_gain)
	my_file.seek(aVERTICAL_OFFSET)
	print("Vertical offset is ",struct.unpack('f',my_file.read(4)))
	my_file.seek(aFIXED_VERT_GAIN)
	print("Fixed vertical gain index is",struct.unpack('h',my_file.read(2)))
	my_file.seek(aNOMINAL_BITS)
	print("Nominal bits is ",struct.unpack('h',my_file.read(2)))
	my_file.seek(aHORIZ_INTERVAL)
	print("Horizontal interval is ",struct.unpack('f',my_file.read(4)))	
	my_file.seek(aHORIZ_OFFSET)
	print("Horizontal offset is ",struct.unpack('d',my_file.read(8)))	

	my_file.seek(aWAVE_DESCRIPTOR)
	wave_descriptor = struct.unpack('i',my_file.read(4))
	print("descriptor is ",wave_descriptor)

	my_file.seek(aUSER_TEXT)
	USER_TEXT			= struct.unpack('i',my_file.read(4))#ReadLong(fid, aUSER_TEXT);
	my_file.seek(aWAVE_ARRAY_1)
	WAVE_ARRAY_1		= struct.unpack('i',my_file.read(4))
	my_file.seek(aWAVE_ARRAY_COUNT)
	WAVE_ARRAY_COUNT    = struct.unpack('i',my_file.read(4))
	my_file.seek(aPNTS_PER_SECREEN)
	PNTS_PER_SCREEN    = struct.unpack('i',my_file.read(4))
	my_file.seek(aTRIGTIME_ARRAY)
	TRIGTIME_ARRAY      = struct.unpack('i',my_file.read(4))

	my_file.seek(aSEGMENT_INDEX)
	SEGMENT_INDEX      = struct.unpack('i',my_file.read(4))
	my_file.seek(aSUBARRAY_COUNT)
	SUBARRAY_COUNT      = struct.unpack('i',my_file.read(4))
	print("Actual segment count: ",SUBARRAY_COUNT)
	my_file.seek(aNOM_SUBARRAY_COUNT)
	NOM_SUBARRAY_COUNT      = struct.unpack('h',my_file.read(2))
	print("Target segment count: ",NOM_SUBARRAY_COUNT)

	my_file.seek(aTRIGGER_TIME)
	TRIGGER_TIME      = struct.unpack('d',my_file.read(8))

	my_file.seek(aACQ_DURATION)
	ACQ_DURATION      = struct.unpack('f',my_file.read(4))

	print("User text ",USER_TEXT)
	print("Wave array",WAVE_ARRAY_1)
	print("Wave array count",WAVE_ARRAY_COUNT)
	print("PNTS_PER_SCREEN",PNTS_PER_SCREEN)
	print("Trig time array",TRIGTIME_ARRAY)
	print("Segment index",SEGMENT_INDEX)
	print("Trigger time,",TRIGGER_TIME)
	print("Acquisition duration",ACQ_DURATION)

	my_file.seek(aFIRST_VALID_PNT)
	FIRST_VALID_PNT = struct.unpack("i",my_file.read(4))

	my_file.seek(aLAST_VALID_PNT)
	LAST_VALID_PNT = struct.unpack("i",my_file.read(4))
	print("First point ",FIRST_VALID_PNT)
	print("LAST point ",LAST_VALID_PNT)
	# b_y_data = my_file.read(WAVE_ARRAY_1[0])
	# exit
	offset = WAVEDESC + wave_descriptor[0] + USER_TEXT[0] #+ TRIGTIME_ARRAY[0]
	my_file.seek(offset)
	print(offset)
	time_event1      = struct.unpack('d',my_file.read(8))
	offset_event1      = struct.unpack('d',my_file.read(8))

	#my_file.seek(offset + 1000+ TRIGTIME_ARRAY[0])
	time_event2      = struct.unpack('d',my_file.read(8))
	offset_event2      = struct.unpack('d',my_file.read(8))

	time_event3      = struct.unpack('d',my_file.read(8))
	offset_event3      = struct.unpack('d',my_file.read(8))

	print("time event 1 ",time_event1)
	print("offset event 1 ",offset_event1)
	print("time event 2 ",time_event2)
	print("offset event 2 ",offset_event2)
	print("time event 3 ",time_event3)
	print("offset event 3 ",offset_event3)


	my_file.seek(offset + TRIGTIME_ARRAY[0])
	b_y_data = my_file.read(1004)
	y_axis = struct.unpack("<"+str(502)+"h", b_y_data)
	data = [1000*vertical_gain*y for y in y_axis]
	#for y in data:
	#	print "%.2f" %y


def get_waveform_block_offset(filepath_in):
	my_file = open(filepath_in, 'rb')

	my_file.seek(aUSER_TEXT)
	USER_TEXT = struct.unpack('i',my_file.read(4))#ReadLong(fid, aUSER_TEXT);
	my_file.seek(aTRIGTIME_ARRAY)
	TRIGTIME_ARRAY = struct.unpack('i',my_file.read(4))
	my_file.seek(aWAVE_DESCRIPTOR)
	WAVE_DESCRIPTOR = struct.unpack('i',my_file.read(4))

	offset = WAVEDESC + WAVE_DESCRIPTOR[0] + USER_TEXT[0] #+ TRIGTIME_ARRAY[0]
	full_offset = WAVEDESC + WAVE_DESCRIPTOR[0] + USER_TEXT[0] + TRIGTIME_ARRAY[0]
	my_file.close()
	return offset,full_offset


def get_configuration(filepath_in):
	my_file = open(filepath_in, 'rb')
	my_file.seek(aVERTICAL_GAIN)
	vertical_gain = struct.unpack('f',my_file.read(4))[0]
	my_file.seek(aVERTICAL_OFFSET)
	vertical_offset = struct.unpack('f',my_file.read(4))[0]
	my_file.seek(aHORIZ_INTERVAL)
	horizontal_interval = struct.unpack('f',my_file.read(4))[0]
	my_file.seek(aSUBARRAY_COUNT)
	nsegments      = struct.unpack('i',my_file.read(4))[0]
	my_file.seek(aWAVE_ARRAY_COUNT)
	WAVE_ARRAY_COUNT    = struct.unpack('i',my_file.read(4))[0]
	points_per_frame = int(WAVE_ARRAY_COUNT / nsegments)
	my_file.close()
	return [nsegments,points_per_frame,horizontal_interval,vertical_gain,vertical_offset]


def get_segment_times(filepath_in,offset,nsegments):
	my_file = open(filepath_in, 'rb')
	trigger_times = []
	horizontal_offsets = []

	my_file.seek(offset)
	for i_event in range(nsegments):
		trigger_times.append(struct.unpack('d',my_file.read(8))[0])
		horizontal_offsets.append(struct.unpack('d',my_file.read(8))[0])
	
	my_file.close()
	return trigger_times,horizontal_offsets


def get_vertical_array(filepath_in,full_offset,points_per_frame,vertical_gain,vertical_offset,event_number):
	my_file = open(filepath_in, 'rb')

	starting_position = full_offset + 2*points_per_frame*event_number
	my_file.seek(starting_position)
	binary_y_data = my_file.read(2*points_per_frame)
	y_axis_raw = struct.unpack("<"+str(points_per_frame)+"h", binary_y_data)
	y_axis = [vertical_gain*y - vertical_offset for y in y_axis_raw]

	my_file.close()
	return y_axis


def calc_horizontal_array(points_per_frame,horizontal_interval,horizontal_offset):
	x_axis = horizontal_offset + horizontal_interval * np.linspace(0, points_per_frame-1, points_per_frame)
	return x_axis


def convert_run(run_number, raw_dir, output_dir, nchan=NCHAN_DEFAULT, prefix="legacy"):
    """Convert Lecroy TRC files to ROOT using legacy unpacking logic."""
    print("[Warning] Using conversion script version: LEGACY")

    start_global = time.time()

    # Gather input files
    inputFiles = [os.path.join(raw_dir, f"C{i+1}--Trace{run_number}.trc")
                  for i in range(nchan)]
    print(f"[Legacy] Run {run_number}, found {len(inputFiles)} channels")

    # Configuration
    vertical_gains, vertical_offsets = [], []
    for infile in inputFiles:
        nsegments, points_per_frame, h_int, v_gain, v_off = get_configuration(infile)
        vertical_gains.append(v_gain)
        vertical_offsets.append(v_off)

    print(f"[Legacy] Segments: {nsegments}, Points/frame: {points_per_frame}")

    # Get trigger times and offsets from each channel
    offset, full_offset = get_waveform_block_offset(inputFiles[0])
    trig_offsets = []
    for infile in inputFiles:
        _, offsets = get_segment_times(infile, offset, nsegments)
        trig_offsets.append(offsets)

    # Prepare output ROOT file
    os.makedirs(output_dir, exist_ok=True)
    outputFilePath= os.path.join(output_dir, f"{prefix}_converted_run{run_number}.root")
    outRoot = ROOT.TFile(outputFilePath, "RECREATE")
    outTree = ROOT.TTree("pulse", "pulse")

    # Buffers
    i_evt = np.zeros(1, dtype="u4")
    segment_time = np.zeros(1, dtype="f")
    channel = np.zeros([nchan, points_per_frame], dtype=np.float32)
    time_array = np.zeros([1, points_per_frame], dtype=np.float32)
    time_offsets = np.zeros(nchan, dtype=np.float32)

    # Branches
    outTree.Branch("i_evt", i_evt, "i_evt/i")
    outTree.Branch("segment_time", segment_time, "segment_time/F")
    outTree.Branch("channel", channel, f"channel[{nchan}][{points_per_frame}]/F")
    outTree.Branch("time", time_array, f"time[1][{points_per_frame}]/F")
    outTree.Branch("timeoffsets", time_offsets, f"timeoffsets[{nchan}]/F")

    # Fill tree
    for i in range(nsegments):
        if i % 1000 == 0:
            print(f"[Legacy] Processing event {i}")

        for ch in range(nchan):
            channel[ch] = get_vertical_array(
                inputFiles[ch], full_offset, points_per_frame,
                vertical_gains[ch], vertical_offsets[ch], i
            )
            time_offsets[ch] = trig_offsets[ch][i] - trig_offsets[0][i]

        time_array[0] = calc_horizontal_array(points_per_frame, h_int, trig_offsets[0][i])
        i_evt[0] = i
        segment_time[0] = trig_offsets[0][i]

        outTree.Fill()

    outTree.Write()
    outRoot.Close()

    print(f"[Legacy] Wrote {outputFilePath}")
    print(f"[Legacy] Done in {time.time()-start_global:.1f} s")

    return outputFilePath

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Legacy Lecroy TRC to ROOT converter")
    parser.add_argument("--runNumber", type=int, required=True, help="Run number to process")
    parser.add_argument("--rawDir", type=str, default="output/DEBUG_RUN/raw",
                        help="Directory with TRC files")
    parser.add_argument("--outDir", type=str, default="output/DEBUG_RUN/converted",
                        help="Output directory for ROOT file")
    parser.add_argument("-p", "--prefix", type=str, default="legacy",
                        help="Prefix tag for output file naming")
    parser.add_argument("--nchan", type=int, default=NCHAN_DEFAULT,
                        help="Number of channels to process")
    args = parser.parse_args()

    convert_run(args.runNumber, args.rawDir, args.outDir,
                nchan=args.nchan, prefix=args.prefix)