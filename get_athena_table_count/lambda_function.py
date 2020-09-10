import json
import boto3
import time
import os


def lambda_handler(event, context):
    # bucketName = event['bucketName']
    # database = event['database']
    # startIndex = event['startIndex']
    # endIndex = event['endIndex']
    bucketName = "s3://athena-query-results-bazaarvoice"
    database = "whlau_daas_uat_djrms_db"
    table = "dj_sub_brand"
    query = "SELECT COUNT(*) FROM " + table
    print(query)

    return AthenaQueryToS3(bucketName,database,query)


def AthenaQueryToS3(s3Bucket,database,query):
    ## Creating the Client for Athena
    client = boto3.client('athena')

    ## This function executes the query and returns the query execution ID
    queryResponse = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
            },       
        ResultConfiguration={
            'OutputLocation': s3Bucket,
            }
        )

    ## This function takes query execution id as input and returns the details of the query executed
    response_get_query_details = client.get_query_execution(
        QueryExecutionId = queryResponse['QueryExecutionId']
    )

    print(response_get_query_details)
    time.sleep(3)

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
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "error": response
                    }),
                }
            elif status == 'SUCCEEDED':
                location = response['QueryExecution']['ResultConfiguration']['OutputLocation']

                response_query_result = client.get_query_results(
                    QueryExecutionId = queryResponse['QueryExecutionId']
                )
                result_data = response_query_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
                #print(response_query_result)
                
                print(result_data)
                print("**********")
                #count = json.loads(result_data)
                print(response)

                return result_data


                # return {
                #     "statusCode": 200,
                #     "body": json.dumps({
                #         "csv": oldKey,
                #         "data": result_data
                #     }),
                # }
        time.sleep(10)
