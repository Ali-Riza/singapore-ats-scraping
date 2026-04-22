#!/bin/bash


set -euo pipefail


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


echo "Fetching and resetting to latest changes..."
git fetch origin || { echo "git fetch failed"; exit 1;}
git reset --hard origin/main || { echo "git reset failed"; exit 1; }


# Nach dem Pull genau einmal neu starten,
# damit wirklich die aktuelle run.command ausgeführt wird
if [ "${RUN_COMMAND_RESTARTED:-0}" != "1" ]; then
echo
echo "Restarting launcher to apply updates..."
export RUN_COMMAND_RESTARTED=1
exec ./run.command
fi


echo
echo "Installing dependencies..."
python3 -m pip install -r requirements.txt || { echo "pip install failed"; exit 1; }


echo
echo "Installing Playwright Chromium..."
python3 -m playwright install chromium || { echo "Playwright install failed"; exit 1; }


echo
echo "Select input Excel file..."
INPUT_FILE="$(osascript <<'APPLESCRIPT'
set selectedFile to choose file with prompt "Select input Excel file for run_pipeline" of type {"xlsx", "xlsm", "xls"}
POSIX path of selectedFile
APPLESCRIPT
)" || { echo "No file selected. Aborting."; exit 1; }


if [ -z "$INPUT_FILE" ]; then
echo "No file selected. Aborting."
exit 1
fi


if [ ! -f "$INPUT_FILE" ]; then
echo "Selected file does not exist: $INPUT_FILE"
exit 1
fi


echo
echo "Running script with input: $INPUT_FILE"
python3 -m src.runners.run_pipeline --input "$INPUT_FILE" || { echo "Script failed"; exit 1; }


echo
echo "Done!"
read -n 1 -s -r -p "Press any key to close..."
echo