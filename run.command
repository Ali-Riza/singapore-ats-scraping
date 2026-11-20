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
echo "Preparing virtual environment (.venv)..."
# Homebrew-Python verbietet pip-Installationen ins System (PEP 668).
# Deshalb laeuft alles in einer eigenen .venv im Projektordner.
# Bevorzugt eine bewaehrte Python-Version, weil sehr neue Versionen
# fuer gepinnte Pakete (pandas, lxml) oft noch keine fertigen Wheels haben.
pick_python() {
for cand in python3.13 python3.12 python3.11 python3; do
if command -v "$cand" >/dev/null 2>&1; then
echo "$cand"
return 0
fi
done
return 1
}

# Vorhandene .venv nur verwenden, wenn sie noch funktioniert
# (nach einem Homebrew-Update kann ihr Python verschwunden sein).
if [ -d ".venv" ] && ! .venv/bin/python -c "import sys" >/dev/null 2>&1; then
echo "Existing .venv is broken, recreating it..."
rm -rf .venv
fi

if [ ! -d ".venv" ]; then
BASE_PY="$(pick_python)" || { echo "No suitable Python found."; exit 1; }
echo "Creating .venv with $BASE_PY ($("$BASE_PY" --version))"
"$BASE_PY" -m venv .venv || { echo "Creating .venv failed"; exit 1; }
fi

PY=".venv/bin/python"
echo "Using $("$PY" --version) from .venv"

echo
echo "Installing dependencies..."
"$PY" -m pip install --upgrade pip >/dev/null 2>&1 || true
"$PY" -m pip install -r requirements.txt || { echo "pip install failed"; exit 1; }


echo
echo "Installing Playwright Chromium..."
"$PY" -m playwright install chromium || { echo "Playwright install failed"; exit 1; }


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
"$PY" -m src.runners.run_pipeline --input "$INPUT_FILE" || { echo "Script failed"; exit 1; }


echo
echo "Done!"
read -n 1 -s -r -p "Press any key to close..."
echo