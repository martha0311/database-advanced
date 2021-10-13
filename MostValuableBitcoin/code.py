from pip._vendor.requests import get
import json
from random import randint
from bs4 import BeautifulSoup
import re
import csv
import pandas as pd
import time

"""Returns information about the most valuable hash per bitcoin per minute via Scraping
Parameters:
  None
Return Values:
  hash (String)
  Time (string)
  amount (BTC) (String)
  amount (USD) (String)
"""
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
    splitthem = stripspace.split(" ")
    
    hashes = open("hashes.txt", "a")
    hashes.write(splittem)
    hashes.write("\n")
    hashes.close()
    
    startTime = time.time()
    time.sleep(60.0) - ((time.time() - startTime) % 60.0)