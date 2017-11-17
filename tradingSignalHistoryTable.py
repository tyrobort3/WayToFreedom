import boto3

from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

class TradingSignalHistoryTable(object):
	dynamodb = boto3.resource('dynamodb')

	def __init__(self, tableName):
		self.tableName = tableName
		self.tradingSignalHistoryTable = self.dynamodb.Table(tableName)

	def updateBuyingSignalHistory(self, candidates):
		for candidate in candidates:
			self.tradingSignalHistoryTable.put_item(
				Item = {
					'MarketName': candidate[1]['pair'],
					'TimeStamp': str(datetime.now()),
					'SignalType': 'Buy',
					'CurrentPrice': str(candidate[1]['currPrice']),
					'TwentyFourHourBTCVolume': str(candidate[1]['twentyFourHourBTCVolume']),
					'CurrentTS': str(candidate[1]['currentTS']),
					'DynamicBalanceFactor': str(candidate[1]['dynamicBalanceFactor'])
				}
			)
		
		return

	def updateSellingSignalHistory(self, candidates):
		for candidate in candidates:
			self.tradingSignalHistoryTable.put_item(
				Item = {
					'MarketName': candidate[1]['pair'],
					'TimeStamp': str(datetime.now()),
					'SignalType': 'Sell',
					'CurrentTS': str(candidate[1]['currentTS']),
					'ComPrice': str(candidate[1]['comPrice'])
				}
			)
		
		return