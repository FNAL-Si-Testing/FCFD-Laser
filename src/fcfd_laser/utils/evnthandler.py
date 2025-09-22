
# src/fcfd_laser/utils/evnthandler.py

def GetNextNumber(run_num_file):
    FileHandle = open(run_num_file)
    nextNumber = int(FileHandle.read().strip())
    FileHandle.close()
    FileHandle = open(run_num_file,"w")
    FileHandle.write(str(nextNumber+1)+"\n") 
    FileHandle.close()
    return nextNumber

def ResetRunNumber(run_num_file):
    FileHandle = open(run_num_file,"w")
    FileHandle.write("1"+"\n") 
    FileHandle.close()

def GetLatestNumber(run_num_file):
    FileHandle = open(run_num_file)
    latestNumber = int(FileHandle.read().strip())
    FileHandle.close()
    return latestNumber