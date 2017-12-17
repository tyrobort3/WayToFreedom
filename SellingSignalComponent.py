from __future__ import print_function

import os
import json
import urllib2
import boto3

from datetime import datetime, timedelta
from bittrexQuery import Bittrex
from holdingStatusTable import HoldingStatusTable
from tradingSignalHistoryTable import TradingSignalHistoryTable

HOLDINTSTATUSTABLENAME = os.environ['holdingStatusTableName']
TRADINGSIGNALHISTORYTABLENAME = os.environ['tradingSignalHistoryTableName']
TRADINGSNS = os.environ['tradingSNS']
EXECUTIONSNS = os.environ['executionSNS']
INDIVIDUALSUMMARYPREFIX = os.environ['individualSummaryPrefix']
INDIVIDUALSUMMARYPOSTFIX = os.environ['individualSummaryPostfix']

# Trading logic parameters
STOP_LOSS = float(os.environ['stop_loss'])
STOP_PEAK_LOSS = float(os.environ['stop_peak_loss'])
STOP_GAIN = int(os.environ['stop_gain'])
LOW_MOVEMENT_CHECK_TIME_GAP = int(os.environ['low_movement_check_time_gap'])
LOW_PURCHASE_QUANTITY = float(os.environ['low_purchase_quantity'])
PEAK_PRICE_TRAILING_INTERVALS = [float(i) for i in os.environ['peak_price_trailing_intervals'].split(',')]
PEAK_PRICE_TRAILING_THRESHOLD = [float(i) for i in os.environ['peak_price_trailing_threshold'].split(',')]
PEAK_PRICE_TRAILING_THRESHOLD_GRACE = [float(i) for i in os.environ['peak_price_trailing_threshold_grace'].split(',')]
GRACE_PERIOD = int(os.environ['grace_period'])
GRACE_PERIOD_STOP_LOSS = float(os.environ['grace_period_stop_loss'])
peakPriceTrailingThreshold_grace

holdingStatusTable = HoldingStatusTable(HOLDINTSTATUSTABLENAME)
tradingSignalHistoryTable = TradingSignalHistoryTable(TRADINGSIGNALHISTORYTABLENAME)
bittrex = Bittrex()

def validateBittrex(rawMarketData):
	checkResult = rawMarketData['success']
	if not ('True' == str(checkResult)):
		raise Exception('Error: Validation failed! Failed to connect to Bittrex!')
	else:
		print('Validation passed! Connected to Bittrex')

def retrieveMarketHistoricalData():
	marketHistoricalData = dict()
	holdingPairs = holdingStatusTable.getHoldingPairs()
	print('All holding pairs at the moment:' + str(holdingPairs))
	print('Total holding count is:' + str(len(holdingPairs)))
	
	for pair in holdingPairs:
		print('Start to retrieve data for ' + str(pair))
		values = {'market': pair}
		contents = bittrex.query('getticker', values)
		print('Tickker info for ' + pair + ' is: ')
		print(json.dumps(contents))
		marketHistoricalData[pair] = contents['result']
	
	return marketHistoricalData

def updatePeakValue():
	marketHistoricalData = dict()
	listOfMarket = holdingStatusTable.getHoldingPairs()

	timeStop = str(datetime.now() - timedelta(hours = 2)).replace(' ', 'T')
	
	for market in listOfMarket:
		print('Start to retrieve data for ' + str(market))
		individualMarketUrl = INDIVIDUALSUMMARYPREFIX + market + INDIVIDUALSUMMARYPOSTFIX
		unfilledData = json.loads(urllib2.urlopen(individualMarketUrl).read())['result']

		try:
			it = next(i for i in xrange(len(unfilledData)) if unfilledData[i]['T'] >= timeStop)
		except:
			print(market + ": no valid data within last " + str(2) + " hours and will skip update peak price")
		else:
			cutUnfilledData = unfilledData[it:]
			marketHistoricalData[market] = cutUnfilledData

	holdingStatusTable.updatePeakPrice(marketHistoricalData)

	return

def triggerTradingSNS(sellingingCandidates):
	sns = boto3.client(service_name="sns")
	topicArn = TRADINGSNS
	print('Trading is triggered!')
	print('In ' + TRADINGSNS.split(':')[-1])
	print(sellingingCandidates)
	sns.publish(
		TopicArn = topicArn,
		Message = 'Trading is triggered ' + TRADINGSNS.split(':')[-1] + '!\n' + 'SellingCandidates: ' + str(sellingingCandidates) + '\n'
	)
	return

def triggerExecutionSNS(sellingCandidates):
	sns = boto3.client(service_name="sns")
	topicArn = EXECUTIONSNS
	
	formattedBuyingCandidates = []
	
	formattedSellingCandidates = []
	for candidate in sellingCandidates:
		sellingCandidate = {}
		sellingCandidate['pair'] = candidate[1]['pair']
		sellingCandidate['comPrice'] = candidate[1]['comPrice']
		formattedSellingCandidates.append(sellingCandidate)
	
	print('Execution is triggered!')
	print(json.dumps(formattedSellingCandidates))
	
	message = {
		'message': 'Execution is triggered!',
		'buyingCandidates': formattedBuyingCandidates,
		'sellingCandidates': formattedSellingCandidates
	}
	
	sns.publish(
        TopicArn = topicArn,
		Message = json.dumps({'default': json.dumps(message)}),
		MessageStructure='json'
	)

	return

def generateSellCandidates(marketHistoricalData):
	import heapq as hq
	import time
	import calendar
	import datetime
	if marketHistoricalData==None:
		raise ValueError('erroneous marketHistoricalData')
	sellCand=[]
	for pair in marketHistoricalData.keys():
		holdingStatus=holdingStatusTable.getHoldingStatus(pair)
		currTS=calendar.timegm(datetime.datetime.utcnow().utctimetuple())
		ans=sellSig(
			holdingStatus=holdingStatus,
			currPrice=marketHistoricalData[pair]['Last'],
			currTS=currTS,
			thresholds={
				'stopLoss':STOP_LOSS,
				'stopPeakLoss':STOP_PEAK_LOSS,
				'stopGain':STOP_GAIN,
				'lowMovementCheckTimeGap':LOW_MOVEMENT_CHECK_TIME_GAP,
				'LowPurchaseQuantity':LOW_PURCHASE_QUANTITY
			},
			peakPriceTrailingIntervals=PEAK_PRICE_TRAILING_INTERVALS,
			peakPriceTrailingThreshold=PEAK_PRICE_TRAILING_THRESHOLD,
			gracePeriod=GRACE_PERIOD,
			gracePeriodStopLoss=GRACE_PERIOD_STOP_LOSS,
			peakPriceTrailingThreshold_grace=PEAK_PRICE_TRAILING_THRESHOLD_GRACE
		)
		if ans!=None and ans['sig']!=None:
			hq.heappush(sellCand,(-ans['sig'],{'comPrice':ans['comPrice'],'pair':pair,'currentTS':calendar.timegm(datetime.datetime.utcnow().utctimetuple())}))
	return sellCand


def sellSig(holdingStatus,currPrice,currTS,thresholds={'stopLoss':-0.07,'stopPeakLoss':-0.1,'stopGain':0.2,'lowMovementCheckTimeGap':60,'LowPurchaseQuantity':0.001},peakPriceTrailingIntervals=[0.1,0.2],peakPriceTrailingThreshold=[0.5,0.6,0.7],gracePeriod=30,gracePeriodStopLoss=-0.1,peakPriceTrailingThreshold_grace=[0.5,0.6,0.7]):
	import sys
	import calendar
	import datetime
	import time
	if holdingStatus==None or holdingStatus['HoldingStatus']=='False':
		return None
	if holdingStatus['BuyPrice']==None or currPrice==None or thresholds==None:
		raise ValueError('erroneous holdingStatus('+str(holdingStatus['BuyPrice'])+') OR currPrice('+str(currPrice)+') OR thresholds('+str(thresholds)+')')
	if len(peakPriceTrailingIntervals)<=0 or len(peakPriceTrailingIntervals)!=len(set(peakPriceTrailingIntervals)):
		raise ValueError('erroneous peakPriceTrailingIntervals: '+str(peakPriceTrailingIntervals))
	if len(peakPriceTrailingThreshold)<=0 or len(peakPriceTrailingThreshold)!=len(peakPriceTrailingIntervals)+1:
		raise ValueError('erroneous peakPriceTrailingThreshold: '+str(peakPriceTrailingThreshold)+' OR peakPriceTrailingIntervals: '+str(peakPriceTrailingIntervals))
	if len(peakPriceTrailingThreshold_grace)<=0 or len(peakPriceTrailingThreshold_grace)!=len(peakPriceTrailingIntervals)+1:
		raise ValueError('erroneous peakPriceTrailingThreshold_grace: '+str(peakPriceTrailingThreshold_grace)+' OR peakPriceTrailingIntervals: '+str(peakPriceTrailingIntervals))
	if holdingStatus['BuyPrice']<=0 or currPrice<0:
		raise ValueError('erroneous holdingStatus('+str(holdingStatus)+') OR currPrice('+str(currPrice)+')')
	if gracePeriod==None or gracePeriod<0:
		raise ValueError('erroneous gracePeriod'+str(gracePeriod))
	if gracePeriodStopLoss==None or gracePeriodStopLoss>0:
		raise ValueError('erroneous gracePeriodStopLoss'+str(gracePeriodStopLoss))

	#
	holdingStatus['BuyPrice']=float(holdingStatus['BuyPrice'])
	holdingStatus['PeakPrice']=float(holdingStatus['PeakPrice'])

	if currTS-calendar.timegm(datetime.datetime.strptime(holdingStatus['CreatedTimeStamp'],"%Y-%m-%d %H:%M:%S.%f").timetuple())<=gracePeriod*60:
		if (currPrice-holdingStatus['BuyPrice'])<=gracePeriodStopLoss*holdingStatus['BuyPrice']:
			return {'sig':sys.maxint,'comPrice':(1-abs(gracePeriodStopLoss))*holdingStatus['BuyPrice']}
		pPTT=peakPriceTrailingThreshold_grace
	else:
		if (currPrice-holdingStatus['BuyPrice'])<=thresholds['stopLoss']*holdingStatus['BuyPrice']:
			return {'sig':sys.maxint,'comPrice':(1-abs(thresholds['stopLoss']))*holdingStatus['BuyPrice']}
		pPTT=peakPriceTrailingThreshold
	# if (currPrice-holdingStatus['PeakPrice'])/holdingStatus['PeakPrice']<=thresholds['stopPeakLoss']:
	# 	return sys.maxint
	# if (currPrice-holdingStatus['BuyPrice'])/holdingStatus['BuyPrice']>=thresholds['stopGain']:
	# 	return sys.maxint

	peakPriceTrailingIntervals.sort()
	peakPriceTrailingIntervals=([-sys.maxint] if peakPriceTrailingIntervals[0]>-sys.maxint else [])+peakPriceTrailingIntervals+([sys.maxint] if peakPriceTrailingIntervals[-1]<sys.maxint else [])
	
	if holdingStatus['PeakPrice']>holdingStatus['BuyPrice']:
		risePct=(holdingStatus['PeakPrice']-holdingStatus['BuyPrice'])/holdingStatus['BuyPrice']
		for i in range(1,len(peakPriceTrailingIntervals)):
			if peakPriceTrailingIntervals[i-1]<risePct<=peakPriceTrailingIntervals[i]:
				if pPTT[i-1]>=0:
					comPrice=(1-pPTT[i-1])*holdingStatus['BuyPrice']+pPTT[i-1]*holdingStatus['PeakPrice']
				else:
					comPrice=(1+pPTT[i-1])*holdingStatus['BuyPrice']
				if currPrice<=comPrice:
					print('info: peak price trailing conditions: ',holdingStatus['PeakPrice'],holdingStatus['BuyPrice'],currPrice,peakPriceTrailingIntervals[i-1],peakPriceTrailingIntervals[i],pPTT[i-1],comPrice)
					return {'sig':sys.maxint,'comPrice':comPrice}
	# if (currTS - holdingStatus['buyTimeStamp']>thresholds['lowMovementCheckTimeGap']*60) and (floor((currTS - holdingStatus['buyTimeStamp'])/86400) * price change threshold %  > (last price / buy price - 1) )
	# if holdingStatus['BuyPrice']*holdingStatus['Q']<thresholds['LowPurchaseQuantity']:
	# 	print('info: LowPurchaseQuantity',holdingStatus,thresholds['LowPurchaseQuantity'])
	# 	return sys.maxint
	return None




def lambda_handler(event, context):
	try:
		# Validate Bittrex connection
		rawMarketSummaryData = bittrex.query('getmarketsummaries')
		validateBittrex(rawMarketSummaryData)

		# RetrieveMarketHistoricalData
		marketHistoricalData = retrieveMarketHistoricalData()

		# Update peak price for holding pairs
		updatePeakValue()

		# Generate selling candidates
		sellingingCandidates = generateSellCandidates(marketHistoricalData)
		print('sellingCandidates:', sellingingCandidates)
		if (len(sellingingCandidates) != 0):
			triggerTradingSNS(sellingingCandidates)
			triggerExecutionSNS(sellingingCandidates)

		# Update sell signal history
		tradingSignalHistoryTable.updateSellingSignalHistory(sellingingCandidates)

	except Exception, e:
		print('Error: ' + str(e))
		raise
	else:
		print('Selling signal generated successfully')
		return event['time']
	finally:
		print('Selling signal generating complete at {}'.format(str(datetime.now())))
