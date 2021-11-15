import re
import csv
import time
import json
import redis
import pandas as pd
import pymongo as mongo
from requests import get
from bs4 import BeautifulSoup

url = 'https://www.blockchain.com/btc/unconfirmed-transactions'
res = get(url).text
soup = BeautifulSoup(res, 'html.parser')
allHashes = soup.find_all("div", attrs={"class", "sc-1g6z4xm-0 hXyplo"})

listAllHashes = []
hashElement = []

for allHash in allHashes:
    listAllHashes.append(allHash.get_text())

while True: 
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
    
    header = ["Hash", "Time", "BTC", "USD"]
    with open("transaction.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(hashElement)
    file.close()
    
    datafile = pd.read_csv("transaction.csv")
    
    col = datafile["USD"]
    maximumValue = col.max()
    mostValuable = datafile.loc[datafile["USD"] == maximumValue]
    mostValuableString = mostValuable.to_string()
    space_nextline = mostValuableString.split("\n")
    
    jointhem = "".join([str(element) for element in space_nextline[1]])
    removNumLine = re.sub(r"^\d* ", "", jointhem)
    stripspace = removNumLine.lstrip()
    splitthem = stripspace.split("  ")

    ## Connecting to redis with authentication
    redisClient =redis.Redis(
        host = 'localhost',
        port = 6379,
        db = 0,
    )

    ## setting the keys and values to redis for 1 minute
    redisClient.set(
        "Hash", splitthem[0], ex = 60
    )
    redisClient.set(
        "Time", splitthem[1], ex = 60
    )
    redisClient.set(
        "BTC", splitthem[2], ex = 60
    )
    redisClient.set(
        "USD", splitthem[3], ex = 60
    )

    ## getting the values from redis
    redisHash = redisClient.get(
        "Hash"
    )
    redisTime = redisClient.get(
        "Time"
    )
    redisBTC = redisClient.get(
        "BTC"
    )
    redisUSD = redisClient.get(
        "USD"
    )
    
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
    
    startTime = time.time()
    time.sleep(60.0 - ((time.time() - startTime) % 60.0))