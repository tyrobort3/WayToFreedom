import boto3

from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

class HoldingStatusTable(object):
	dynamodb = boto3.resource('dynamodb')

	def __init__(self, tableName):
		self.tableName = tableName
		self.holdingStatusTable = self.dynamodb.Table(tableName)

	# Set holding status for market name with holding status, buy price and peak price
	def setHoldingStatus(self, marketName, holdingStatus, buyPrice, peakPrice):
		newPeakPrice = str()
		newBuyPrice = str()
		createdTimeStamp = str()
		if (holdingStatus == 'False'):
			self.holdingStatusTable.delete_item(
				Key={
					'MarketName': marketName
				}
			)
			return
		else:
			currentHoldingStatus = self.getHoldingStatus(marketName)
			currentTimeStamp = str(datetime.now())
	
			if (currentHoldingStatus is not None):
				# Update peak Price
				if (float(currentHoldingStatus['PeakPrice']) >= peakPrice):
					newPeakPrice = currentHoldingStatus['PeakPrice']
				else:
					newPeakPrice = str(peakPrice)
	
				# Will not update buy price
				if (currentHoldingStatus['BuyPrice'] == '0'):
					newBuyPrice = str(buyPrice)
				else:
					newBuyPrice = currentHoldingStatus['BuyPrice']
	
				# Will not update created time stamp
				createdTimeStamp = currentHoldingStatus['CreatedTimeStamp']
			else:
				newBuyPrice = str(buyPrice)
				newPeakPrice = str(peakPrice)
				createdTimeStamp = str(currentTimeStamp)
	
		self.holdingStatusTable.put_item(
			Item = {
				'MarketName': marketName,
				'CreatedTimeStamp': createdTimeStamp,
				'HoldingStatus': holdingStatus,
				'BuyPrice': newBuyPrice,
				'PeakPrice': newPeakPrice,
				'LatestPeakPriceTimeStamp': currentTimeStamp
			}
		)
		return

	# get holding status for market name
	def getHoldingStatus(self, marketName):
		response = self.holdingStatusTable.query(
			KeyConditionExpression=Key('MarketName').eq(marketName)
		)
	
		if (len(response['Items']) != 1):
			return None
		else:
			return response['Items'][0]

	# update peak price for holding pairs
	def updatePeakPrice(self, marketHistoricalData):
		timeStop = str(datetime.now() - timedelta(minutes = 55)).replace(' ', 'T')
		response = self.holdingStatusTable.scan(
			FilterExpression=Key('HoldingStatus').eq('True')
		)

		for holdingPair in response['Items']:
			pair = holdingPair['MarketName']
			historicalData = marketHistoricalData[pair]
			potentialPeakPrice = None

			for data in (data for data in historicalData if data['T'] >= timeStop):
				if (potentialPeakPrice is None):
					potentialPeakPrice = data['C']
				elif (potentialPeakPrice < data['C']):
					potentialPeakPrice = data['C']

			if (potentialPeakPrice is not None):
				print(pair + ' with new peak price: ' + str(potentialPeakPrice))
				self.setHoldingStatus(pair, 'True', potentialPeakPrice, potentialPeakPrice)

		return

	# Return all holding pairs
	def getHoldingPairs(self):
		response = self.holdingStatusTable.scan(
				FilterExpression=Key('HoldingStatus').eq('True')
			)
		holdingPairs = list()
		for holdingPair in response['Items']:
			holdingPairs.append(holdingPair['MarketName'])

		return holdingPairs