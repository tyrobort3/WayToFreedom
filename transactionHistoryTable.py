import boto3
import json

from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

class TransactionHistoryTable(object):
	dynamodb = boto3.resource('dynamodb')

	def __init__(self, tableName):
		self.tableName = tableName
		self.transactionHistoryTable = self.dynamodb.Table(tableName)

	def updateBuyingTransactionHistory(self, pair, quantity, rate, details):
		timeStamp = str(datetime.now())
		tradingType = 'Buy'
		self.transactionHistoryTable.put_item(
			Item = {
				'MarketName': pair,
				'TimeStamp': timeStamp,
				'TradingType': tradingType,
				'Quantity': str(quantity),
				'Rate': str(rate),
				'TradingDetails': json.dumps((details))
			}
		)
		
		return

	def updateSellingTransactionHistory(self, pair, quantity, rate, details):
		timeStamp = str(datetime.now())
		tradingType = 'Sell'
		self.transactionHistoryTable.put_item(
			Item = {
				'MarketName': pair,
				'TimeStamp': timeStamp,
				'TradingType': tradingType,
				'Quantity': str(quantity),
				'Rate': str(rate),
				'TradingDetails': json.dumps((details))
			}
		)
		
		return