import requests
from requests.exceptions import ConnectionError
import os
from io import BytesIO
import zipfile
import yaml

from app.helpers import logs


def download_file(config_path) -> None:

    with open(config_path) as conf_file:
        config = yaml.safe_load(conf_file)

    _url = config['crawler']['url']
    _certificado = config['crawler']['certificado']
    _path_data = config['crawler']['data']

    logs('Criando o diretorio de dados..')
    os.makedirs(_path_data , exist_ok=True)
    os.system(f'rm -rf {_path_data }/*')

    try:
        logs('Requisitando os dados..')
        response = requests.get(_url, verify=_certificado)
        
        logs('Requisicao bem sucedida..')
        filebytes = BytesIO(response.content)

        logs('Descompactando o arquivo zip..')
        myzip = zipfile.ZipFile(filebytes)
        myzip.extractall(_path_data)

        logs('Arquivo descompactado com sucesso!')
    
    except ConnectionError:
        logs('Erro de conexao.. Limpano o diretorio de dados..')
        os.system(f'rm -rf {_path_data }')
        exit()