import os
from dotenv import load_dotenv
load_dotenv()
from e2b_code_interpreter import Sandbox
import glob
from pathlib import Path

os.system("clear")

sandbox = Sandbox("tv90caqgg0pxcdmvt9rr", timeout=300)
print(f"Sandbox ID: {sandbox.sandbox_id}")


# Get all files in the data directory
data_files = glob.glob("src/analyze_data/data/*")

# Copy each data file to the sandbox
for file_path in data_files:
    file_name = os.path.basename(file_path)
    print(f"Copying {file_path} to sandbox...")
    with open(file_path, "rb") as file:
        file_content = file.read()
        sandbox.files.write(f"/home/user/data/{file_name}", file_content)

with open("src/e2b_code/processing_script.py", "rb") as file:
    file_content = file.read()
    print(f"Copying processing_script.py to sandbox...")
    sandbox.files.write("/home/user/processing_script.py", file_content)

with open(".env", "rb") as file:
    file_content = file.read()
    print(f"Copying .env to sandbox...")
    sandbox.files.write("/home/user/.env", file_content)

print("Running script inside the e2b sandbox...")
result = sandbox.commands.run("python3 /home/user/processing_script.py")
print(result)

print("Execution inside the sandbox completed. Downloading output files...")

# Create local output directory
local_output_dir = Path("fitness_output")
local_output_dir.mkdir(parents=True, exist_ok=True)

# List files in the sandbox output directory to see what was created
files_list = sandbox.commands.run("ls -la /home/user/fitness_output")
print("Files created in the sandbox:")
print(files_list.stdout)

# Download files using the simpler approach from documentation
try:
    # Always attempt to download the recommendation file
    recommendation_content = sandbox.files.read("/home/user/fitness_output/training_recommendation.txt")
    recommendation_path = local_output_dir / "training_recommendation.txt"
    with open(recommendation_path, "w") as file:
        file.write(recommendation_content)
    print(f"Downloaded: {recommendation_path}")
    
    # Try to download the calendar files if they exist
    for filename in ["training_plan.ics", "training_plan.csv"]:
        try:
            content = sandbox.files.read(f"/home/user/fitness_output/{filename}")
            local_path = local_output_dir / filename
            with open(local_path, "w") as file:
                file.write(content)
            print(f"Downloaded: {local_path}")
        except Exception as e:
            print(f"Could not download {filename}: {e}")
            
except Exception as e:
    print(f"Error downloading files: {e}")

print("Done!")