#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 09:59:21 2020

@author: fidelg
"""


import csv
import pandas as pd
import datetime
import argparse
import MySQLdb
#I may have to alter the table Reg5Min at some point to 



parser = argparse.ArgumentParser()
parser.add_argument("startDate", type=str, help="From date, format 'YYY-MM-D'")
parser.add_argument("endDate", type=str, help="To date, format 'YYYY-MM-D'")
#parser.add_argument("protocol", type=str, help="the csv file with the protocol configuration")
#parser.add_argument("-f", "--verbosity", action="count", default=0)
args = parser.parse_args()

#print(args.startDate)
#print(args.endDate)

try:
	start_date_obj = datetime.datetime.strptime(args.startDate, '%Y-%m-%d')
	end_date_obj = datetime.datetime.strptime(args.endDate, '%Y-%m-%d')
except:
	print("Please use the correct format, try '-h' option")
	exit()

#print(end_date_obj,start_date_obj)

if end_date_obj > start_date_obj:
	#print("I can proceed")
	dates=pd.date_range(start_date_obj,end_date_obj).astype(str).tolist()
	#print(dates)
	db = MySQLdb.connect('127.0.0.1','ICI','pickering','ICILocal')
	cursor = db.cursor()
	for date in dates:
		#print(date)
		sql= "INSERT INTO date4FTP(previousDate) VALUES('%s')"%(date)
		cursor.execute(sql)
		db.commit()

else:
	print("I cannot proceed, start date later than end date given")
db.close()
#print("Importing data from: ",row[0])
#GatherDataFromPT2K.data_from_PT2K(row[0])
