import subprocess
import sys
from datetime import datetime

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def run_script(script_name):
    log(f"Starting {script_name}...")
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd="/app/src",
            capture_output=False,
            text=True
        )
        if result.returncode == 0:
            log(f"Completed {script_name}")
        else:
            log(f"Error in {script_name}")
            return False
    except Exception as e:
        log(f"Exception in {script_name}: {e}")
        return False
    return True

def main():
    log("Starting pipeline")
    
    scripts = [
        "bronze_ingest.py",
        "silver_clean.py",
        "gold_aggregate.py"
    ]
    
    for script in scripts:
        if not run_script(script):
            log(f"Pipeline failed at {script}")
            return
    
    log("Pipeline completed successfully")

if __name__ == "__main__":
    main()
