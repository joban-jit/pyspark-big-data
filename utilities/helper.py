from pathlib import Path
import os


def get_data_file_path(file_path_in_data_folder):
    # data_path = Path.cwd().parent / 'data'
    # data_path = Path.cwd().absolute()
    # print(Path.home())
    # print(os.path.dirname(os.path.abspath(__file__)))
    # print(os.path.dirname(os.path.realpath("/data/"+file_path_in_data_folder)))
    data_folder = f"{Path.home()}/PycharmProjects/PySparkWithBigData/data/"
    result = f'{str(data_folder)}{file_path_in_data_folder}'
    return result


if __name__ == "__main__":
    get_data_file_path("ml-100k/data.u")
