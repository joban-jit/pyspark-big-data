from pathlib import Path


def get_data_file_path(file_path_in_data_folder):
    data_folder = Path("PySparkWithBigData/data/")
    # data_path = Path.cwd().parent / 'data'
    # data_path = Path.cwd().absolute()
    return f'{str(data_folder)}/{file_path_in_data_folder}'

# if __name__=="__main__":
#     print(get_data_file_path("hi"))