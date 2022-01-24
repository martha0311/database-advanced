#This code didn't run either

import re
import csv
import time
import json
import redis
import pickle
import pandas as pd
import pymongo as mongo
from requests import get
from bs4 import BeautifulSoup

while True: 

    url = 'https://www.blockchain.com/btc/unconfirmed-transactions'
    res = get(url).text
    soup = BeautifulSoup(res, 'html.parser')
    allHashes = soup.find_all("div", attrs={"class", "sc-1g6z4xm-0 hXyplo"})

    listAllHashes = []
    hashElement = []

    for allHash in allHashes:
        listAllHashes.append(allHash.get_text())

    for element in listAllHashes:
        elementcopy1 = re.sub("Hash", "", element)
        elementcopy2 = re.sub(",", "", elementcopy1)
        elementcopy3 = re.sub("Amount", "", elementcopy2)
        elementcopy4 = re.sub("Time", " ", elementcopy3)
        elementcopy5 = re.sub("[\b()\b]", "", elementcopy4)
        elementcopy6 = re.sub("[\bBTC\b]", "", elementcopy5)
        elementcopy7 = re.sub("[\bUSD$\b]", "", elementcopy6)
        elementcopy8 = re.sub("  ", " ", elementcopy7)
        hashElement.append(elementcopy8.split(" "))
    
    ## Connecting to redis with authentication
    redisClient =redis.Redis(
        host = 'localhost',
        port = 6379,
        db = 0,
        charset="utf-8",
        decode_responses=True
    )

    key = 0

    ## Connecting to redis with authentication
    for hashEle in hashElement:
        key += 1

        dictionary = {"Hash": hashEle[0], "Time": hashEle[1],
        "BTC": hashEle[2], "USD": hashEle[3]}

        redisClient.set(key, pickle.dumps(dictionary))

    temporary = 0 

    for key in redisClient.scan_iter():
        data = pickle.loads(redisClient.get(key))
        usd = 'USD' in data

        if usd > temporary:
            temporary = usd 

    for akey in redisClient.scan_iter():
        datainfo = pickle.loads(redisClient.get(akey))
        redisUSD = 'USD' in datainfo

        if redisUSD == temporary:
            redisHash = 'Hash' in datainfo
            redisTime = 'Time' in datainfo
            redisBTC = 'BTC' in datainfo

            print(redisUSD)

    ## Connecting to Mongo without security
    mongoclient = mongo.MongoClient("mongodb://localhost:27017")

    ## Database
    bitcoinDatabase = mongoclient["bitcoindatabase"]

    ## Collection
    valuableCollection = bitcoinDatabase["valuablecollection"]

    ## Data 
    valuableBitcoin = { 
        "Hash": redisHash, 
        "Time": redisTime, 
        "BTC": redisBTC, 
        "USD": redisUSD
    }

    ## Inserting data 
    valuable = valuableCollection.insert_one(valuableBitcoin)

    for k in redisClient.scan_iter():
        redisClient.delete(k)
    
    startTime = time.time()
    time.sleep(60.0 - ((time.time() - startTime) % 60.0))