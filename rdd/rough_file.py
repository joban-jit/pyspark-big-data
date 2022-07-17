from pathlib import Path
import os

data_path = Path.cwd().parent/'data'
file_path = "/ml-100k/u.data"

full_path_to_data = str(data_path)+file_path
print(full_path_to_data)
print(str(os.getcwd()))
