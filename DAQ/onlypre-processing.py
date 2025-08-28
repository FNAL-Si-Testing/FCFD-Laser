import subprocess
import os


# directories/paths to files
sh_script_path = "/home/arcadia/Documents/Motors_automation_test/TimingDAQ/script_FCFD.sh"
BASE_PATH = "/home/arcadia/Documents/Motors_automation_test/DAQtest" # this is for the file indecxing txt file

def GetNextNumber():
    run_num_file = BASE_PATH + "/next_run_number.txt"
    FileHandle = open(run_num_file)
    nextNumber = int(FileHandle.read().strip())
    FileHandle.close()
    FileHandle = open(run_num_file,"w")
    FileHandle.write(str(nextNumber+1)+"\n") 
    FileHandle.close()
    return nextNumber

# the range of runs
start_index = 1
end_index = 150

# writes the starting run number intor the next_run_number.txt file
# run_num_file = BASE_PATH + "/next_run_number.txt"
# FileHandle = open(run_num_file)
# nextNumber = int(FileHandle.read().strip())
# FileHandle.close()
# FileHandle = open(run_num_file,"w")
# FileHandle.write(str(start_index)+"\n") 
# FileHandle.close()


# # pre-processes the runs from start_index to end_index
# for i in range(start_index, end_index + 1):  # start_index to end_index inclusive
#     try:
#         latest_run_number = str(GetNextNumber())
#         subprocess.run(["bash", sh_script_path, latest_run_number], check=True)
#         print("DAQ pipeline ok.")

#     except Exception as e:
#         print(f"shell hook failed: {e}")




try:
    # latest_run_number = str(GetNextNumber())
    latest_run_number = str(150)
    subprocess.run(["bash", sh_script_path, latest_run_number], check=True)
    print("DAQ pipeline ok.")

except Exception as e:
    print(f"shell hook failed: {e}")