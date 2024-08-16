import subprocess
import sys
import os
from etl import main

# Install required libraries to run the project
def install_requirements():
    try:
        # Get the directory of the current script (etl.py)
        current_dir = os.path.dirname(__file__)
        # Construct the path to the project root by going up one level from src/
        project_root = os.path.abspath(os.path.join(current_dir, '..'))
        # Build the full path to the .env file in the config directory
        requirements_path = os.path.join(project_root, 'requirements.txt')
        # Run the pip install command
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_path])
        print("Requirements installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install requirements: {e}")
        sys.exit(1)

# Running the actual code which is stored in the main function fro mthe etl.py file
if __name__ == "__main__":
    install_requirements()
    main()