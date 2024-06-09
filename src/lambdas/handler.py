import boto3
import hashlib
import json
import os
from datetime import datetime

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')



def lambda_handler(event, context):
    bucket_name = 'ai-technical-test-ibio-escobar'
    table_name = 'ai-technical-test-ibio-escobar'
    
    # Obtener el nombre del archivo del evento
    file_key = event['Records'][0]['s3']['object']['key']
    
    # Obtener el archivo del bucket de S3
    file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = file_obj['Body'].read().decode('utf-8')
    
    # Parsear el contenido del archivo
    data = parse_file_content(file_content)
    
    # Generar el HASH MD5
    generated_hash = generate_md5_hash(data)
    
    # Validar el HASH
    if generated_hash != data['hash']:
        raise ValueError("HASH mismatch! Data integrity check failed.")
    
    # Preparar los datos para DynamoDB
    dynamodb_item = {
        'timestamp': {'S': str(datetime.utcnow())},
        'totalContactoClientes': {'N': str(data['totalContactoClientes'])},
        'motivoReclamo': {'N': str(data['motivoReclamo'])},
        'motivoGarantia': {'N': str(data['motivoGarantia'])},
        'motivoDuda': {'N': str(data['motivoDuda'])},
        'motivoCompra': {'N': str(data['motivoCompra'])},
        'motivoFelicitaciones': {'N': str(data['motivoFelicitaciones'])},
        'motivoCambio': {'N': str(data['motivoCambio'])}
    }
    
    
    # Almacenar los datos en DynamoDB
    dynamodb_client.put_item(TableName=table_name, Item=dynamodb_item)
    
    # Eliminar el archivo de S3
    s3_client.delete_object(Bucket=bucket_name, Key=file_key)
    
    return {
        'statusCode': 200,
        'body': json.dumps('File processed successfully')
    }

def parse_file_content(content):
    lines = content.strip().split('\n')
    data = {}
    for line in lines:
        key, value = line.split('=')
        data[key] = int(value) if key != 'hash' else value
    return data

def generate_md5_hash(data):
    concatenated_string = f"{data['totalContactoClientes']}~{data['motivoReclamo']}~{data['motivoGarantia']}~{data['motivoDuda']}~{data['motivoCompra']}~{data['motivoFelicitaciones']}~{data['motivoCambio']}"
    return hashlib.md5(concatenated_string.encode('utf-8')).hexdigest()
