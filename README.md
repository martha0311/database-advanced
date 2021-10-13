# My Package  

This is a readme file that informs you that this package scrapes blockchain.com to find the most valuable hash for bitcoin per minute.

# Installation  

pip install wheel  

pip install twine 

# Usage 

from pip._vendor.requests import get => used for sending a request, to get a response. 

import json => used to transfer data as text/ store or read the following types: strings, intergers, floats, booleans, lists, etc. 

from bs4 import BeautifulSoup => helps in getting data out of html.

regex, csv, time and pandas were also used.

## code result

This package scrapes blockchain and store all the information of the unconfirmed transactions for that minute in a list as a string. The unconfirmed transactions has four variables; hash, time, amount (BTC) and amount (USD), all stored in an element of the list with their values. 

The most valueable hash per minute is found by finding the maximum amount of USD per minute. 

The most valuable hash for bitcoin per minute is then written in the hash.txt file, with its variables; hash, time, amount (BTC) and amount (USD).

## Licence  

[MIT] (https://choosealicense.com/licenses/mit/)N 