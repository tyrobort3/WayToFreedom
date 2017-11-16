import json
import urllib
import urllib2
import time
import hmac
import hashlib

class Bittrex(object):
	public = ['getmarkets', 'getcurrencies', 'getticker', 'getmarketsummaries', 'getmarketsummary', 'getorderbook', 'getmarkethistory']
	market = ['buylimit', 'buymarket', 'selllimit', 'sellmarket', 'cancel', 'getopenorders']
	account = ['getbalances', 'getbalance', 'getdepositaddress', 'withdraw', 'getorder', 'getorderhistory', 'getwithdrawalhistory', 'getdeposithistory']

	def __init__(self, KEY=None, SECRET=None):
		self.KEY = KEY
		self.SECRET = SECRET

	def query(self, method, values={}):
		if method in self.public:
			url = 'https://bittrex.com/api/v1.1/public/'
		elif method in self.market:
			url = 'https://bittrex.com/api/v1.1/market/'
		elif method in self.account: 
			url = 'https://bittrex.com/api/v1.1/account/'
		else:
			return 'Something went wrong, sorry.'
			
		url += method + '?' + urllib.urlencode(values)
			
		if method not in self.public:
			url += '&apikey=' + self.KEY
			url += '&nonce=' + str(int(time.time()))
			signature = hmac.new(self.SECRET, url, hashlib.sha512).hexdigest()
			headers = {'apisign': signature}
		else:
			headers = {}
		
		request = urllib2.Request(url, headers=headers)
		response = json.loads(urllib2.urlopen(request).read())
		
		return response