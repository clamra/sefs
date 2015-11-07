'''
DB
'''
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, GlobalAllIndex
from config import mailbox_table_name

class Mailbox(object):
    mailbox_table_config = {
        "schema": [
            HashKey('address'), # defaults to STRING data_type
        ], 
        "throughput" : {
            'read': 1,
            'write': 1,
        }, 
        "global_indexes" : [
            GlobalAllIndex('forward', 
                parts=[
                    HashKey('forward'),
                ],
                throughput={
                    'read': 1,
                    'write': 1,
                }
            )
        ],
    }

    @classmethod
    def get_table(cls):
        return Table(mailbox_table_name, **cls.mailbox_table_config)

    @classmethod
    def init(cls):
        Table.create(mailbox_table_name, **cls.mailbox_table_config)
        # If you need to specify custom parameters, such as credentials or region,
        # use the following:
        # connection=boto.dynamodb2.connect_to_region('us-east-1')
        
    @classmethod
    def put_item(cls, address, forward, **kwargs):
        cls.get_table().put_item(data=dict({
            "address" : address,
            "forward" : forward,
        }, **kwargs))

    @classmethod
    def batch_get(cls, addresses):
        return cls.get_table().batch_get(keys=[
            {"address" : address} for address in addresses
        ])
