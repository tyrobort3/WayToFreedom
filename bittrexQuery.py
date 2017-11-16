import json
import urllib
import urllib2
import time
import hmac
import hashlib

public = ['getmarkets', 'getcurrencies', 'getticker', 'getmarketsummaries', 'getmarketsummary', 'getorderbook', 'getmarkethistory']
market = ['buylimit', 'buymarket', 'selllimit', 'sellmarket', 'cancel', 'getopenorders']
account = ['getbalances', 'getbalance', 'getdepositaddress', 'withdraw', 'getorder', 'getorderhistory', 'getwithdrawalhistory', 'getdeposithistory']

def query(method, values={}):
	if method in public:
		url = 'https://bittrex.com/api/v1.1/public/'
	elif method in market:
		url = 'https://bittrex.com/api/v1.1/market/'
	elif method in account: 
		url = 'https://bittrex.com/api/v1.1/account/'
	else:
		return 'Something went wrong, sorry.'
		
	url += method + '?' + urllib.urlencode(values)
		
	if method not in public:
		url += '&apikey=' + KEY
		url += '&nonce=' + str(int(time.time()))
		signature = hmac.new(SECRET, url, hashlib.sha512).hexdigest()
		headers = {'apisign': signature}
	else:
		headers = {}
	
	request = urllib2.Request(url, headers=headers)
	response = json.loads(urllib2.urlopen(request).read())
	
	return response