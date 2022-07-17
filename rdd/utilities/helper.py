from pathlib import Path


def get_data_file_path(file_path_in_data_folder):
    data_path = Path.cwd().parent / 'data'
    return f'{str(data_path)}/{file_path_in_data_folder}'
