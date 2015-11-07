'''
AWS Lambda Handler that forward emails received by SES
'''

import logging
import re
from config import email_store_bucket, email_store_prefix, ses_region
from models import Mailbox

import boto
from boto.s3.key import Key
import boto.ses


logging.basicConfig(level=logging.INFO)

def get_forward_addresses(event):
    recipients = event['Records'][0]['ses']['receipt']['recipients']
    logging.info('origin recipients %s', recipients);
    forward_to = [
        item['forward']
        for item in Mailbox.batch_get(recipients)
    ]
    logging.info('forward to %s', forward_to);
    return forward_to


def get_raw_message(message_id):
    conn = boto.connect_s3()
    bucket = conn.get_bucket(email_store_bucket)
    k = Key(bucket)
    k.key = email_store_prefix + message_id 
    return k.get_contents_as_string()


def do_forward(event, forward_addresses):
    email = event['Records'][0]['ses']['mail']
    recipients = event['Records'][0]['ses']['receipt']['recipients']
    message = get_raw_message(email['messageId'])

    #rewrite message 
    def replace_func(match):
        from_addr = match.group(0)
        return 'From: ' + from_addr.replace('<', '(').replace('>', ')') + ' via ' + recipients[0] + ' <' + recipients[0] + '>'
    message = re.compile(r"^From: (.*)", re.M).sub(replace_func, message)
    message = re.compile(r"^Sender: (.*)", re.M).sub("Sender: " + recipients[0], message)

    ses_conn = boto.ses.connect_to_region(ses_region)
    ses_conn.send_raw_email(message, source=recipients[0], destinations=forward_addresses)


def handler(event):
    forward_addresses = get_forward_addresses(event)
    if len(forward_addresses) == 0:
        logging.info("can't found forward address , drop the email")
        return
    do_forward(event, forward_addresses)

def main():
    import json
    import sys
    event = json.load(sys.stdin)
    handler(event)

if __name__ == "__main__":
    main()
