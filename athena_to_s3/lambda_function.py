import json
import boto3
import csv
import time
import os
import utilities



def lambda_handler(event, context):
    input = event
    payload = ""

    if "Payload" in input:
        input = json.loads(event['Payload'])
        payload = event['Payload']

    S3_BUCKET = input['BUCKET_NAME']
    DATABASE = input['DATABASE']
    QUERY = input['QUERY']
    OUTPUT_FILE_VARIABLE = input['OUTPUT_FILE_VARIABLE']

    ## Creating the Client for Athena
    client = boto3.client('athena')

    print("Attempting to run query: " + QUERY)

    ## This function executes the query and returns the query execution ID
    queryResponse = client.start_query_execution(
        QueryString=QUERY,
        QueryExecutionContext={
            'Database': DATABASE
            },       
        ResultConfiguration={
            'OutputLocation': S3_BUCKET,
            }
        )

    ## This function takes query execution id as input and returns the details of the query executed
    response_get_query_details = client.get_query_execution(
        QueryExecutionId = queryResponse['QueryExecutionId']
    )

    print(response_get_query_details)
    time.sleep(20)

    status = 'QUEUED'
    iterations = 5

    while (iterations > 0 and status in ['RUNNING', 'QUEUED']):
        print("Number of polls left: " + str(iterations))
        max_execution = iterations - 1
        response = client.get_query_execution(QueryExecutionId = queryResponse['QueryExecutionId'])

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:

            status = response['QueryExecution']['Status']['State']
            print("Status: " + status)
            if status == 'FAILED' or status == 'CANCELLED':
                payload = utilities.AddOrUpdateJsonWithValue('message', response, payload)
                return False
            elif status == 'SUCCEEDED':
                location = response['QueryExecution']['ResultConfiguration']['OutputLocation']

                response_query_result = client.get_query_results(
                    QueryExecutionId = queryResponse['QueryExecutionId']
                )
                result_data = response_query_result['ResultSet']
                print("location: ", location)
        time.sleep(10)

    payload = utilities.AddOrUpdateJsonWithValue(OUTPUT_FILE_VARIABLE, location, payload)

    return { 'Payload' : payload } 