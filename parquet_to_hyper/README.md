# Try Yourself
1. Open PowerShell as Admin
2. Create a python virtual environment to install necessary packages:  
`cd <your desired direction of the virtual environment>`  
`python -m venv p2h`  
`cd p2h\Scripts\`  
`.\activate.ps1`  
`pip install -r <path to the requirement.txt in this repository>`
3. [Optional] create some sample parquet files by running  
`python create_parquet_file.py`  
This will create a folder of partitioned parquets in `C:\p2h\sample\<uuid4>`
4. Run the parquet to hyper script  
`python convert_parquet_file.py <input parquet folder/file path> <output hyper file path>`
5. For example  
`python .\convert_parquet_to_hyper.py "C:\p2h\sample\16525c42-d031-45d2-bb69-67ce2f766631" "C:\p2h\output.hyper"`
6. Done!