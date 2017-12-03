from __future__ import print_function

import os
import json
import urllib2
import boto3

from datetime import datetime, timedelta
from bittrexQuery import Bittrex
from holdingStatusTable import HoldingStatusTable
from tradingSignalHistoryTable import TradingSignalHistoryTable

MARKETLIMIT = int(os.environ['marketLimit'])
HOURINTEREST = int(os.environ['hourInterest'])
INDIVIDUALSUMMARYPREFIX = os.environ['individualSummaryPrefix']
INDIVIDUALSUMMARYPOSTFIX = os.environ['individualSummaryPostfix']
HOLDINTSTATUSTABLENAME = os.environ['holdingStatusTableName']
TRADINGSIGNALHISTORYTABLENAME = os.environ['tradingSignalHistoryTableName']
TRADINGSNS = os.environ['tradingSNS']
EXECUTIONSNS = os.environ['executionSNS']

# Trading logic parameters
CHECK_TIME_STAMP_WINDOW = [int(i) for i in os.environ['check_time_stamp_window'].split(',')]
PRICE_THRESHOLD_WINDOW = [float(i) for i in os.environ['price_threshold_window'].split(',')]
VOLUME_TEST_WINDOW = int(os.environ['volume_test_window'])
VOLUME_TEST_QUANTITY = float(os.environ['volume_test_quantity'])
LAST_WINDOW_PRICE_INCREASE_THRESHOLD = float(os.environ['last_window_price_increase_threshold'])
LAST_WINDOW_MOMENTUM_THRESHOLD = float(os.environ['last_window_momentum_threshold'])
LAST_24_HOURS_BASE_VOLUME = float(os.environ['last_24_hours_base_volume'])

holdingStatusTable = HoldingStatusTable(HOLDINTSTATUSTABLENAME)
tradingSignalHistoryTable = TradingSignalHistoryTable(TRADINGSIGNALHISTORYTABLENAME)
bittrex = Bittrex()


def validateBittrex(rawMarketData):
	checkResult = rawMarketData['success']
	if not ('True' == str(checkResult)):
		raise Exception('Error: Validation failed! Failed to connect to Bittrex!')
	else:
		print('Validation passed! Connected to Bittrex')

def retrieveMarketHistoricalData(rawMarketSummaryData):
	marketHistoricalData = dict()
	print('Total number of market is {}'.format(str(len(rawMarketSummaryData['result']))))
	listOfMarket = getListOfMarket(rawMarketSummaryData)
	print('Filtered number of market is {}'.format(str(len(listOfMarket))))
	
	timeStop = str(datetime.now() - timedelta(hours = HOURINTEREST)).replace(' ', 'T')
	
	marketLimit = MARKETLIMIT
	for market in listOfMarket:
		print('Start to retrieve data for ' + str(market))
		individualMarketUrl = INDIVIDUALSUMMARYPREFIX + market + INDIVIDUALSUMMARYPOSTFIX
		unfilledData = json.loads(urllib2.urlopen(individualMarketUrl).read())['result']

		try:
			it = next(i for i in xrange(len(unfilledData)) if unfilledData[i]['T'] >= timeStop)
		except:
			print(market + ": no valid data within last " + str(HOURINTEREST) + " hours")
		else:
			cutUnfilledData = unfilledData[it:]
			startTimeStamp = cutUnfilledData[-1]['T']
			mostRecentData = getMostRecentData(market, startTimeStamp)
			cutUnfilledData.extend(mostRecentData)
			marketHistoricalData[market] = cutUnfilledData
			marketHistoricalData[market] = cutUnfilledData
		finally:
			# Need to break before running limit
			marketLimit = marketLimit - 1
			if (marketLimit <= 0):
				break
	
	return marketHistoricalData
	
def getListOfMarket(rawMarketSummaryData):
	listOfMarket = list()
	for record in rawMarketSummaryData['result']:
		tradingPair = record['MarketName']
		if (tradingPair.startswith('BTC')) and (float(record['BaseVolume']) > LAST_24_HOURS_BASE_VOLUME):
			listOfMarket.append(record['MarketName'])
	
	return listOfMarket

def getMostRecentData(market, startTimeStamp):
	try:
		mostRecentData = list()
		values = {'market': market}
		contents = bittrex.query('getmarkethistory', values)['result']
		
		# Only need to calculate time later than start time stamp
		it = next(i for i in xrange(len(contents)) if (startTimeStamp >= contents[i]['TimeStamp']))
		filteredTransactionData = contents[:it]
		
		currentTimeStamp = startTimeStamp
		O = 0.0
		C = 0.0
		H = 0.0
		L = 0.0
		V = 0.0
		BV = 0.0
		
		for record in reversed(filteredTransactionData):
			timeStamp = record['TimeStamp'].split('.')[0][:-2]
			
			if (currentTimeStamp < timeStamp):
				# Need write to result for last accumulated result
				if (currentTimeStamp != startTimeStamp):
					mostRecentData.append(
						{
							'O' : O,
							'C' : C,
							'H' : H,
							'L' : L,
							'V' : V,
							'BV' : BV,
							'T' : currentTimeStamp
						}
					)
					
				# Start new accumulation
				currentTimeStamp = timeStamp + '00'
				O = record['Price']
				C = record['Price']
				H = record['Price']
				L = record['Price']
				V = record['Quantity']
				BV = record['Total']
			else:
				H = max(H, record['Price'])
				L = min(L, record['Price'])
				V += record['Quantity']
				BV += record['Total']
	except Exception, e:
		print('Error: ' + str(e))
		raise
	
	return mostRecentData

def triggerTradingSNS(buyingCandidates):
	sns = boto3.client(service_name="sns")
	topicArn = TRADINGSNS
	print('Trading is triggered!')
	print('In ' + TRADINGSNS.split(':')[-1])
	print(buyingCandidates)
	sns.publish(
		TopicArn = topicArn,
		Message = 'Trading is triggered ' + TRADINGSNS.split(':')[-1] + '!\n' + 'BuyingCandidates: ' + str(buyingCandidates) + '\n'
	)
	return

def triggerExecutionSNS(buyingCandidates):
	sns = boto3.client(service_name="sns")
	topicArn = EXECUTIONSNS
	
	formattedBuyingCandidates = []
	for candidate in buyingCandidates:
		buyingCandidate = {}
		buyingCandidate['pair'] = candidate[1]['pair']
		buyingCandidate['buyPrice'] = candidate[1]['currPrice']
		buyingCandidate['dynamicBalanceFactor'] = candidate[1]['dynamicBalanceFactor']
		formattedBuyingCandidates.append(buyingCandidate)

	formattedSellingCandidates = []
	
	print('Execution is triggered!')
	print(json.dumps(formattedBuyingCandidates))
	
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

def generateBuyCandidates(marketHistoricalData):
	import heapq as hq
	import time
	import calendar
	import datetime
	if marketHistoricalData==None:
		raise ValueError('erroneous marketHistoricalData')
	buyCand=[]
	for pair in marketHistoricalData.keys():
		ans=rollingWindow_2(
			tradingPair=pair,
			data=marketHistoricalData[pair],
			histTimeInterval=1,
			warningTimeGap=10,
			maxLatency=5,
			checkTS=CHECK_TIME_STAMP_WINDOW,
			Pthres=PRICE_THRESHOLD_WINDOW,
			Vtimespan=VOLUME_TEST_WINDOW,
			Vthres=VOLUME_TEST_QUANTITY,
			lastPthres=LAST_WINDOW_PRICE_INCREASE_THRESHOLD,
			lastWinMomentumThres=LAST_WINDOW_MOMENTUM_THRESHOLD,
			maxPriceTimeSpan=24*60
		)
		if ans!=None and ans['buySig']!=None:
			hq.heappush(buyCand,(-ans['buySig'],{'dynamicBalanceFactor':ans['dynamicBalanceFactor'],'pair':pair,'twentyFourHourBTCVolume':ans['twentyFourHourBTCVolume'],'peakPrice':ans['peakPrice'],'buyPrice':ans['buyPrice'],'currPrice':ans['currPrice'],'currentTS':calendar.timegm(datetime.datetime.utcnow().utctimetuple())}))
	return buyCand

#following are designed to parallel run
def rollingWindow_2(tradingPair,data,histTimeInterval=1,warningTimeGap=60,maxLatency=5,checkTS=[-45,-30,-15],Pthres=[0.0001,0.0001,0.0001],Vtimespan=45,Vthres=50,lastPthres=0.05,lastWinMomentumThres=0.2,maxPriceTimeSpan=24*60):
	#-------------------------------
	#this function is for trading strategy 2
	#the time units are still min
	#Note, code will check current price regardless of checkTS, thus the length of Pthres is equal to checkTS (not less than 1)
	#-------------------------------
	import datetime
	import time
	import calendar
	#import collections as c
	if tradingPair==None:
		print("erroneous tradingPair: "+str(tradingPair))
		return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':None,'twentyFourHourBTCVolume':None,'peakPrice':None,'buyPrice':None,'currPrice':None}
	if data==None or len(data)<=5:
		#here need to check with sell logic, for that if data==None, which means we dont have this pair's history, but this doesn't mean it's not trading (due to lag or anything else), if this's the case we may lose the sell signal
		print("erroneous input data: "+str(data))
		return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':None,'twentyFourHourBTCVolume':None,'peakPrice':None,'buyPrice':None,'currPrice':None}
	#sort data to make sure its time ascending
	data.sort(key=lambda x:x['T'])
	print('latest timeStamp: '+str(tradingPair)+' '+str(data[-1]['T']))
	#check sell signal before everything else
	currPrice,currTS=data[-1]['C'],calendar.timegm(datetime.datetime.strptime(data[-1]['T'],"%Y-%m-%dT%H:%M:%S").timetuple())
	startTS=calendar.timegm(datetime.datetime.strptime(data[0]['T'],"%Y-%m-%dT%H:%M:%S").timetuple())
	#read holding position here
	holdingStatus=holdingStatusTable.getHoldingStatus(tradingPair)
	#deprecated, sell and buy are completely seperated
	sellSignal=None

	if warningTimeGap==None or (not 0<warningTimeGap):
		raise ValueError('warningTimeGap >0')
	if maxPriceTimeSpan==None or (not 0<maxPriceTimeSpan):
		raise ValueError('maxPriceTimeSpan: '+str(maxPriceTimeSpan))
	if histTimeInterval>=warningTimeGap:
		raise ValueError('histTimeInterval: '+str(histTimeInterval)+'must be less than warningTimeGap: '+str(warningTimeGap))
	if maxLatency==None or maxLatency>6:
		raise ValueError('None maxLatency or maxLatency('+str(maxLatency)+') cannot exceed 6min due to dynamic last timeStamp')
	if calendar.timegm(datetime.datetime.utcnow().utctimetuple())-currTS>maxLatency*60:
		print('warning: '+str(tradingPair)+' last update timestamp too old: '+str(data[-1]['T']))
		return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
	if len(checkTS)<=0 or len(checkTS)!=len(Pthres) or len(checkTS)<=2:
		raise ValueError('erroneous checkTS('+str(checkTS)+') or Pthres('+str(Pthres)+')')
	checkTS.sort()
	if checkTS[-1]>=0:
		raise ValueError('last checkTS('+str(checkTS)+') must less than 0')
	if Vtimespan==None or Vtimespan<=0 or Vthres==None or Vthres<=0:
		raise ValueError('erroneous Vtimespan('+str(Vtimespan)+') or Vthres('+str(Vthres)+')')
	Vthres=float(Vthres)
	if startTS-calendar.timegm(datetime.datetime.strptime(data[-1]['T'],"%Y-%m-%dT%H:%M:%S").timetuple())>checkTS[0]*60:
		print('history not exceeding desired check timeStamp: '+str(checkTS[0])+' '+str(data[-1]['T'])+' '+str(data[0]['T']))
		return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
	#initialization
	prices=[None]*len(checkTS)+[float(data[-1]['C'])]
	checkTSunix=[currTS+entry*60 for entry in checkTS]
	checkTSpointer=len(checkTS)-1
	stopTime=currTS+min(checkTS[0],-1*Vtimespan,-maxPriceTimeSpan)*60
	if startTS>stopTime:
		print('warning: trading pair '+str(tradingPair)+' oldest record('+str(data[0]['T'])+') not exceeding stopTime('+str(stopTime)+')')
	BTCVolume,vWindow=float(data[-1]['BV']),{'start':currTS-Vtimespan*60,'end':currTS}
	preTs=currTS
	maxPriceTimeSpan_p=float(data[-1]['C'])
	lastWindowMax,lastWindowMin=prices[-1],prices[-1]
	#start loop
	for i in range(len(data)-2,-1,-1):
		ts=calendar.timegm(datetime.datetime.strptime(data[i]['T'],"%Y-%m-%dT%H:%M:%S").timetuple())
		cp=float(data[i]['C'])
		if cp<=0:
			print('warning: erroneous data closing price('+str(cp)+')')
			return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}			
		if abs(preTs-ts)>warningTimeGap*60:
			print('warning, '+str(tradingPair)+' time interval exceeds warningTimeGap('+str(warningTimeGap)+') '+str(data[i]['T'])+' '+str(data[i+1]['T']))
			return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
		if abs(preTs-ts)<histTimeInterval*60:
			print(str(data[i-1]))
			print(str(data[i]))
			print('data timestamp overlapping, will skip this trading pair')
			return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
		maxPriceTimeSpan_p=max(maxPriceTimeSpan_p,cp)
		if checkTSpointer>=0 and ts<=checkTSunix[checkTSpointer]:
			if checkTSpointer>0 and ts<=checkTSunix[checkTSpointer-1]:
				print('time gap between data record for trading pair '+str(tradingPair)+' are too big or checkTS intervals are too frequent')
				print(checkTSpointer,checkTSunix[checkTSpointer],checkTSunix[checkTSpointer-1],ts)
				return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
			prices[checkTSpointer]=cp
			if checkTSunix[-1]<=ts<=currTS:
				lastWindowMax=max(lastWindowMax,cp)
				lastWindowMin=min(lastWindowMin,cp)
			if prices[checkTSpointer]<=0:
				print('erroneous '+str(tradingPair)+' closing price: '+str(cp))
				return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
			if (prices[checkTSpointer+1]-prices[checkTSpointer])>Pthres[checkTSpointer]*prices[checkTSpointer]:
				pass
			else:
				print('warning: '+str(tradingPair)+' not passing increasing threshold: prices('+str(prices)+') Pthres('+str(Pthres)+')')
				return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
			checkTSpointer-=1
		if vWindow['start']<=ts<=vWindow['end']:
			BTCVolume+=float(data[i]['BV'])
		if ts<stopTime:
			break
		preTs=ts
	if float(data[-1]['C'])<maxPriceTimeSpan_p:
		print('warning: tradingPair '+str(tradingPair)+' not passing maxPriceTimeSpan('+str(maxPriceTimeSpan)+') maxPriceTimeSpan_p('+str(maxPriceTimeSpan_p)+')')
		return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}		
	if (lastWindowMax-prices[-1])>=lastWinMomentumThres*(lastWindowMax-lastWindowMin):
		print('warning: tradingPair '+str(tradingPair)+' not passing last window momentum threshold('+str(lastWinMomentumThres)+')')
		print(lastWindowMax,lastWindowMin,prices[-1])
		return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}		
	if BTCVolume<Vthres:
		print('warning: tradingPair '+str(tradingPair)+' not passing last Vthres('+str(Vthres)+') BTCVolume('+str(BTCVolume)+')')
		return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
	if prices[0]>0 and (prices[-1]-prices[0])<=lastPthres*prices[0]:
		print(prices)
		print('warning: tradingPair '+str(tradingPair)+' not passing last lastPthres('+str(lastPthres)+')')
		return {'dynamicBalanceFactor':None,'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
	return {'dynamicBalanceFactor':BTCVolume/Vthres,'buySig':BTCVolume/Vthres+(prices[-1]-prices[-2])/prices[-2],'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}



def lambda_handler(event, context):
	try:
		# Validate Bittrex connection
		rawMarketSummaryData = bittrex.query('getmarketsummaries')
		validateBittrex(rawMarketSummaryData)

		# RetrieveMarketHistoricalData
		marketHistoricalData = retrieveMarketHistoricalData(rawMarketSummaryData)

		# Update peak price for holding pairs
		holdingStatusTable.updatePeakPrice(marketHistoricalData)

		# Generate buy signal
		buyingCandidates = generateBuyCandidates(marketHistoricalData)
		# buyingCandidates.append((-2.5845085626548787,{'dynamicBalanceFactor':1.5,'pair':'BTC-DASH','twentyFourHourBTCVolume':None,'peakPrice':None,'buyPrice':None,'currPrice':0.0798913,'currentTS':1510528097}))
		print('buyingCandidates:', buyingCandidates)
		if (len(buyingCandidates) != 0):
			triggerTradingSNS(buyingCandidates)
			triggerExecutionSNS(buyingCandidates)

		# Update buy signal history
		tradingSignalHistoryTable.updateBuyingSignalHistory(buyingCandidates)

	except Exception, e:
		print('Error: ' + str(e))
		raise
	else:
		print('Buying signal generated successfully')
		return event['time']
	finally:
		print('Buying signal generating complete at {}'.format(str(datetime.now())))
