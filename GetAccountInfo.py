from __future__ import print_function

import os
import json
import urllib2
import boto3

from datetime import datetime, timedelta
from bittrexQuery import Bittrex

KEY=os.environ['key']
SECRET=os.environ['secret']
bittrex = Bittrex(KEY, SECRET)


def lambda_handler(event, context):
	try:
		# GetAccountInfo
		accountInfo = bittrex.query('getbalances')
		print(json.dumps(accountInfo))

	except Exception, e:
		print('Error: ' + str(e))
		raise
	else:
		print('Get account info successfully')
		return event['time']
	finally:
		print('Get account info complete at {}'.format(str(datetime.now())))
