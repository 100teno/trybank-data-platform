import subprocess
import sys

def run_step(script_name):
    print(f"Running step: {script_name}")
    result = subprocess.run([sys.executable, f"jobs/{script_name}"])

    if result.returncode != 0: 
        print(f"Error while running {script_name}")
        sys.exit(1)

def main():
    print("Starting TryBank Data Pipeline")
    
    run_step("bronze_to_silver.py")
    run_step("silver_to_gold.py")

    print("\nPipeline completed successfully!")

if __name__ == "__main__":
    main()