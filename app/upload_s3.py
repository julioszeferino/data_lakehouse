import boto3
import yaml

from app.helpers import logs


def upload_file(config_path):
    """
    Funcao que realiza o upload do arquivo para a AWS S3.
    """
    with open(config_path) as conf_file:
        config = yaml.safe_load(conf_file)

    _AWS_ACCESS_KEY_ID = config['aws']['credentials']['aws_key']
    _AWS_SECRET_ACCESS_KEY = config['aws']['credentials']['aws_secret']

    logs("Criando a conexao com o S3..")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=_AWS_SECRET_ACCESS_KEY
    )   

    _file_name = config['s3']['data']['filename']
    _bucket = config['s3']['data']['bucket']
    _object_name = config['s3']['data']['objectname']

    logs("Realizando o upload do arquivo para o S3..")
    s3_client.upload_file(
        _file_name, 
        _bucket, 
        _object_name)

    logs("Upload realizado com sucesso!")