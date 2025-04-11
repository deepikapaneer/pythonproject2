python
CopyEdit
import json
import base64
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('SensorData')

def lambda_handler(event, context):
    for record in event['Records']:
        # Decode Kinesis data (base64-encoded)
        payload = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))

        # Add a derived field (e.g., comfort index)
        payload['comfort_index'] = round(payload['temperature'] - (0.55 * (1 - payload['humidity']/100) * (payload['temperature'] - 14.5)), 2)

        # Store into DynamoDB
        table.put_item(Item=payload)

    return {'statusCode': 200, 'body': 'Success'}
