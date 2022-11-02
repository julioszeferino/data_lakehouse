from datetime import datetime

def logs(*args) -> None:
    '''
    Funcao para gerar logs de execucao..
    :param args: Argumentos a serem logados
    :return: None
    '''
    print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - {args}')