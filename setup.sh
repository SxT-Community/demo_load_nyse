# !/bin/bash

# setup demo files
mkdir ./src
touch ./src/load_nyse.py

mkdir ./.env
touch ./.env/secrets.env

touch ./.gitignore
echo "secrets.env" > ./.gitignore
echo "venv_demo/" >> ./.gitignore
echo "tables/" >> ./.gitignore


# create python virtual environment
python3 -m venv venv_demo
source venv_demo/bin/activate
pip3 install --upgrade pip
pip3 install spaceandtime --upgrade
pip3 install yfinance --upgrade

# run the script and close the virtual environment
. ./src/load_nyse.py
deactivate

echo "complete"