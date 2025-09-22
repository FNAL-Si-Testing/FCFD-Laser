#!/usr/bin/env python3
# fcfd_laser/processing/conversion.py

import subprocess
import os

def run_preprocessor(logger, converted_file_path, processed_file_path, config_file_path,
                     executable_path):
    """
    Calls the external C++ preprocessor ('NetScopeStandaloneDat2Root')
    """
    if not os.path.exists(converted_file_path):
        logger.error(f"[Preprocessing] Input file not found: {converted_file_path}")
        return
    
    if not os.path.exists(executable_path):
        logger.error(f"[Preprocessing] C++ executable not found at: {executable_path}")
        return
    
    command = [
        executable_path,
        f"--input_file={converted_file_path}",
        f"--config={config_file_path}",
        f"--output_file={processed_file_path}",
        "--correctForTimeOffsets=true"
    ]

    logger.info("[Preprocessing] Running C++ preprocessor...")
    logger.info(f"  Command: {' '.join(command)}")

    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True) # Debug: check=False
        # logger.info(f"[Preprocessing] C++ preprocessor finished successfully file written to: {processed_file_path}")
        # if result.stdout:
            # logger.info(f"  Preprocessor Output: {result.stdout}")
        # if result.stderr:
            # logger.warning(f"  Preprocessor STDERR: {result.stderr}")
        
        # Check for success based on the return code AND file existence
        if result.returncode == 0 and os.path.exists(processed_file_path):
            logger.info(f"[Preprocessing] C++ preprocessor finished successfully. File written to: {processed_file_path}")
        elif result.returncode == 0 and not os.path.exists(processed_file_path):
            logger.error(f"[Preprocessing] C++ preprocessor reported success, but the output file was NOT created: {processed_file_path}")
        else:
            logger.error(f"  C++ preprocessor failed to execute.")
            logger.error(f"  Return Code: {result.returncode}")

    except Exception as e:
        logger.error(f"  An unexpected error occurred while running the C++ preprocessor: {e}")

