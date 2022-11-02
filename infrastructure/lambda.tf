# Para subir uma funcao lambda na aws, precisamos pegar tudo o que precisamos
# para subir a funcao lambda e colocar em um arquivo zip.

resource "aws_lambda_function" "executa_emr" {
  filename      = "lambda_function_payload.zip"
  function_name = var.lambda_function_name
  role          = aws_iam_role.lambda.arn 
  handler       = "lambda_function.handler" # nomedoarquivo.nomedafuncao
  memory_size   = 128 # maximo de memoria ram
  timeout       = 30 # tempo maximo de execucao

  source_code_hash = filebase64sha256("lambda_function_payload.zip") # controle de versao da funcao lambda

  runtime = "python3.8" # linguagem para executar a funcao

  tags = {
    IES   = "IGTI"
    CURSO = "EDC"
  }

}