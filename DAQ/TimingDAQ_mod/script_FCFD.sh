#!/bin/bash

# cp /home/arcadia/Documents/Motors_automation_test/DAQtest/Converted_runs_root/converted_run$1.root .

# for raw data + root
# /home/arcadia/Documents/Motors_automation_test/TimingDAQ/NetScopeStandaloneDat2Root --input_file=/home/arcadia/Documents/Motors_automation_test/DAQtest/Converted_runs_root/converted_run$1.root --config=/home/arcadia/Documents/Motors_automation_test/TimingDAQ/LecroyScope_v11.config --output_file=/home/arcadia/Documents/Motors_automation_test/DAQtest/Preprocessed_runs_root/out_run$1.root --correctForTimeOffsets=true

# only for root (without raw data)
/home/arcadia/Documents/Motors_automation_test/TimingDAQ/NetScopeStandaloneDat2Root --input_file=/home/arcadia/Documents/Motors_automation_test/DAQtest/Converted_runs_root/converted_run$1.root --config=/home/arcadia/Documents/Motors_automation_test/TimingDAQ/LecroyScope_v11.config --output_file=/home/arcadia/Documents/Motors_automation_test/DAQtest/pre_proc_without_meas/out_run$1.root --correctForTimeOffsets=true

# cp out_run$1.root /home/arcadia/Documents/Motors_automation_test/DAQtest/Preprocessed_runs_root
# rm *$1*.root
