## Funcoes Lambda
- E um servico de computacao serverless da AWS
- E como se houvesse um servico executando um programa, mas nao preciso configurar o servidor
- Apenas subo o codigo e ele roda sozinho

## Criacao de uma key pair para o EMR no EC2
- Acessar o servico EC2
- Clicar em key pairs
- Create key pair -> 
    - Name: julioszeferino-igti-teste
    - File format: pem

## Verificar a subnet
- Acessar o EMR
- Clicar em um cluster que ja havia sido criado
- Procurar pelo subnet id
- No caso, foi esse aqui: subnet-00073236dde29eda6

## Executar a funcao lambda na aws
- Acessar a pagina das funcoes e clicar na funcao lambda criada
- Clicar Test > New Event 
    Template: hello-world
    Name: eventoteste

    Save changes > Test