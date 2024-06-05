# coding=utf-8
import logging
import uuid
from datetime import datetime
from ckan.plugins import toolkit #type:ignore
import boto3 #type:ignore

log = logging.getLogger(__name__)


def send_sqs_message(user, subject, message):
    # Create SQS client
    sqs = boto3.client('sqs',
                       region_name=toolkit.config.get('ckan.sqs.region_id'),
                       aws_access_key_id=toolkit.config.get('ckan.sqs.access_key'),
                       aws_secret_access_key=toolkit.config.get('ckan.sqs.secret_key')
                       )
    
    #messagesAttributes
    #TODO make contact email adres as config
    message_attributes = { 
            'sender_email':{
                'StringValue': "contact@transportdata.be",
                'DataType':'String' 
            },
            'reciever_email':{
                'StringValue': user.email,
                'DataType': 'String'
            },
            'display_name':{
                'StringValue': user.display_name,
                'DataType': 'String'
            },
            'subject':{
                'StringValue': subject,
                'DataType':'String'
            },
            'timeStamp':{
                'StringValue': datetime.now().strftime("%d-%m-%Y, %H:%M:%S"),
                'DataType': 'String'
            }
    }


    if (user.email is None) or not len(user.email):
        log.warn("No recipient email address available for {0}".format(user.display_name))
    
    else:
        # Send message to SQS queue
        response = sqs.send_message(
            QueueUrl=toolkit.config.get('ckan.sqs.queue_url'),
            MessageGroupId='Notify_admin',
            MessageDeduplicationId=str(uuid.uuid4()),
            MessageAttributes=message_attributes,
            MessageBody=message
        )
        #if response.get("ResponseMetadata") is not None:
        if "ResponseMetadata" in response:
            statusCode = response["ResponseMetadata"].get("HTTPStatusCode")
            if statusCode == 200:
                log.info("Membership Message send to sysAdmin {0}".format(user.display_name))
            else:
                log.warn("Membership Message send returned statuscode {0}".format(str( statusCode)))
                raise Exception("Failed to send membership message. Statuscode {0}".format(str(statusCode)))
        else:
            log.error("ResponseMetadata is missing from the response")
            raise Exception("ResponseMetadata missing in the response")