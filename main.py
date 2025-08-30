from pathlib import Path

from aoietl.process import process
from aoietl.copy_to_fileshare import upload_file_to_share
from aoietl.copy_output_to_blob import copy_fileshare_output_to_blob

BASE = Path(__file__).parent.joinpath("data")
DATA = BASE.parent.joinpath("tests", "data")
BASE_OUT = BASE

def main():
    config = BASE.joinpath("config.yaml")
    print(config)
    process(config, DATA, BASE_OUT)
    #upload_file_to_share(BASE_OUT.joinpath("output"))
    #copy_fileshare_output_to_blob()



if __name__ == "__main__":
    main()

