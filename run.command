#!/bin/bash

set -u

# In Script-Ordner wechseln
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || exit

echo "Project directory: $SCRIPT_DIR"
echo

# Check Git
if ! command -v git >/dev/null 2>&1; then
  echo "Git is not installed. Please run: xcode-select --install"
  exit 1
fi

# Check Python
if ! command -v python3 >/dev/null 2>&1; then
  echo "Python3 is not installed."
  exit 1
fi

echo "Pulling latest changes..."
git pull || { echo "git pull failed"; exit 1; }

echo
echo "Installing dependencies..."
python3 -m pip install -r requirements.txt || { echo "pip install failed"; exit 1; }

echo
echo "Installing Playwright Chromium..."
python3 -m playwright install chromium || { echo "Playwright install failed"; exit 1; }

echo
echo "Running script..."
python3 -m src.runners.run_batch3 || { echo "Script failed"; exit 1; }

echo
echo "Done!"
read -n 1 -s -r -p "Press any key to close..."
echo