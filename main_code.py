import subprocess
import sys
from etl import main

def install_requirements():
    try:
        # Run the pip install command
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Requirements installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install requirements: {e}")
        sys.exit(1)

if __name__ == "__main__":
    install_requirements()
    main()