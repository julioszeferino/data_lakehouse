import boto3

def handler(event, context):
    """
    Lambda function that starts a job flow in EMR.

    :params event: o evento que disparou a função
    :params context: o contexto da função

    Nesta funcao nao estamos interagindo com o evento e o contexto
    """
    # criando um cliente do emr
    client = boto3.client('emr', region_name='us-east-2')

    # criando um cluster spark no emr e passa steps para serem executados
    cluster_id = client.run_job_flow(
                Name='EMR-julioszeferino-IGTI-delta', # nome do cluster
                ServiceRole='EMR_DefaultRole', # role do servico
                JobFlowRole='EMR_EC2_DefaultRole', # role do servico
                VisibleToAllUsers=True, # estara visivel para todos os usuarios
                LogUri='s3://julioszeferino-datalake-projeto01-tf/emr-logs', # caminho dos logs
                ReleaseLabel='emr-6.3.0', # versao do emr
                Instances={
                    'InstanceGroups': [
                        {
                            # instancia master
                            'Name': 'Master nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                        },
                        {
                            # worker 1
                            'Name': 'Worker nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                        }
                    ],
                    'Ec2KeyName': 'julioszeferino-igti-teste', # *ver o quick-notes.md -> criacao keypair
                    'KeepJobFlowAliveWhenNoSteps': True, # manter o cluster ativo sem steps executando
                    'TerminationProtected': False, # nao proteger o cluster de ser deletado
                    'Ec2SubnetId': 'subnet-00073236dde29eda6' # *ver o quick-notes.md -> criacao subnet
                },

                # definindo as aplicacoes do cluster EMR
                Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Hive'},
                    {'Name': 'Pig'},
                    {'Name': 'Hue'},
                    {'Name': 'JupyterHub'},
                    {'Name': 'JupyterEnterpriseGateway'},
                    {'Name': 'Livy'},
                ],

                # definindo os steps do cluster EMR
                Configurations=[{
                    # configuracoes do ambiente spark
                    "Classification": "spark-env",
                    "Properties": {},
                    "Configurations": [{
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                    }]
                },
                    {
                        "Classification": "spark-hive-site",
                        "Properties": {
                            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                        }
                    },
                    {
                        "Classification": "spark-defaults",
                        "Properties": {
                            "spark.submit.deployMode": "cluster",
                            "spark.speculation": "false",
                            "spark.sql.adaptive.enabled": "true",
                            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                        }
                    },
                    {
                        "Classification": "spark",
                        "Properties": {
                            "maximizeResourceAllocation": "true"
                        }
                    }
                ],
                
                StepConcurrencyLevel=1, # concorrencia de steps, apenas 1 por vez
                
                # definicao dos steps
                Steps=[{
                    # STEP 1 -> insert dados
                    'Name': 'Delta Insert do ENEM',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                 '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                 '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 's3://julioszeferino-datalake-projeto01-tf/emr-code/pyspark/delta_spark_insert.py'
                                 ]
                    }
                },
                {
                    # STEP 2 -> update dados
                    'Name': 'Simulacao e UPSERT do ENEM',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                 '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                 '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 's3://julioszeferino-datalake-projeto01-tf/emr-code/pyspark/delta_spark_upsert.py'
                                 ]
                    }
                }],
            )
    
    # retorno da funcao lambda
    return {
        'statusCode': 200,
        'body': f"Started job flow {cluster_id['JobFlowId']}"
    }