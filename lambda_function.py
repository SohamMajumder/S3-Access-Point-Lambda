import boto3
import urllib3
import json

http = urllib3.PoolManager()

def lambda_handler(event, context):
    # Print the Event details in case of debugging
    print("Event:", event)
    
    # 1 & 2 Extract relevant metadata including S3URL out of input event
    event_configuration =  event["configuration"]
    print("Event Configuration: ", event_configuration)
    
    payload = event_configuration["payload"]
    print("payload:", payload)
    
    conv2json = json.loads(payload)
    print(conv2json)
    
    #3 - Collect the Dynamic filters from the S3 > Object Lambda Access Point > Payload
    filter_key = conv2json["FilterKey"]
    filter_value = conv2json["FilterValue"]

    print("Filter Key", filter_key)
    print("Filter Value", filter_value)
    
    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]
    s3_url =        object_get_context["inputS3Url"]

    # 4 - Download S3 File
    response = http.request('GET', s3_url)
    
    original_object = response.data.decode('utf-8')
    as_list = json.loads(original_object)
    print(as_list)
    
    result_list = []

    # 5 - Transform object
    for record in as_list:
        if record[filter_key] == filter_value:
            result_list.append(record)

    # 5 - Write object back to S3 Object Lambda
    s3 = boto3.client('s3')
    s3.write_get_object_response(
        Body=json.dumps(result_list),
        RequestRoute=request_route,
        RequestToken=request_token)

    return {'status_code': 200}
