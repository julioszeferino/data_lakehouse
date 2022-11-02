import os
import argparse
from app.crawler import download_file 
from app.upload_s3 import upload_file

# Definindo o diretorio de trabalho
PATH: str = os.getcwd()

def main(config_path) -> None:
    
    download_file(config_path)

    upload_file('config.yml')


if __name__ == '__main__':

    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--config', dest='config', required=True)
    args = args_parser.parse_args()

    main(config_path=args.config)
