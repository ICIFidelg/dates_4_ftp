import ftplib
import time
import MySQLdb
import csv
import os
import logging
import socket
import subprocess
import pysftp


db = MySQLdb.connect('127.0.0.1','ICI','pickering','ICILocal')
cursor = db.cursor()

hostname=socket.gethostname()
CSVsDir="/root/ICI/CSVs/"
mysqlDir="/var/lib/mysql/ICILocal/"

def CreateCSVs(date2TryE,date2TryW,date2TryG,file2Try):
	myQueryHeaders="SELECT 'TimeStamping','SN','Load label','Reading','Utility','Unit' FROM Reg5MinReadings UNION "
	myQueryE="SELECT TimeStamping,SN,MeterParam,value,'E','kwh' FROM Reg5MinReadings WHERE DATE(TimeStamping)='%s' UNION "%(date2TryE)
	myQueryW="SELECT TimeStamping,SN,MeterParam,value,'W','m3' FROM Reg5MinReadingsW WHERE DATE(TimeStamping)='%s' UNION "%(date2TryW)
	myQueryG="SELECT TimeStamping,SN,MeterParam,value,'G','m3' FROM Reg5MinReadingsG WHERE DATE(TimeStamping)='%s'"%(date2TryG)
	myQueryCSV="INTO OUTFILE '%s'  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"%(file2Try)
	myQueryTotal=myQueryHeaders+myQueryE+myQueryW+myQueryG+myQueryCSV
	#print(myQueryTotal)
	#print("\n")
	db.query(myQueryTotal)
	fileCreated=mysqlDir+file2Try
	copyFile = subprocess.run(["cp",fileCreated,CSVsDir])
	fileCopied=CSVsDir+file2Try
	return fileCopied
	
def uploadTheFile(file2Upload,date2Confirm):
	db.query("SELECT * FROM ftpReports WHERE count=(SELECT MAX(count) FROM ftpReports)")
	r=db.store_result()
	serverArray=r.fetch_row(0,1)
	ftpServer=serverArray[0]["ftpServer"]
	userName=serverArray[0]["userName"]
	password=serverArray[0]["password"]
	ftpDirectory="./"+serverArray[0]["directory"]
	print(ftpServer)
	print(userName)
	print(password)
	print(ftpDirectory)
	"""try:
		session = ftplib.FTP(ftpServer, userName, password)	
	except ConnectionRefusedError:
		logging.fatal("Failed To Connect To The FTP Server {}@{}".format(userName, ftpServer))
		logging.fatal("Aborting Application...")
		pass

	logging.info("Uploading File {} To FTP Server".format(file2Upload))
	file = open(file2Upload, 'rb')  # file to send
	session.cwd(ftpDirectory)
	print(os.path.basename(file2Upload))
	try:
		session.storbinary('STOR ' + os.path.basename(file2Upload), file)  # send the file
	except Exception as e:
		print(e)		
	file.close()  # close file and FTP
	session.quit()
	"""
	with pysftp.Connection(ftpServer,userName,password) as sftp:
		with sftp.cd(ftpDirectory):           # temporarily chdir to allcode
			sftp.put(file2Upload)  	# upload file to allcode/pycode on remote
	#print(date2Confirm)
	sql= "UPDATE date4FTP SET dataUpload='yes' WHERE previousDate='%s'"%(date2Confirm)
	cursor.execute(sql)
	db.commit()
	dateNow = time.strftime("%Y-%m-%d",time.localtime())
	try:
		#Im just going to store the current date for next time I try this
		sql= "INSERT IGNORE INTO date4FTP(previousDate) VALUES('%s')"%(dateNow)
		cursor.execute(sql)
		db.commit()
	except:
		pass
		
db.query("SELECT * FROM date4FTP WHERE dataUpload='no' or dataUpload is NULL")
r=db.store_result()
dateArray=r.fetch_row(0,1)
datestoUpload=[]


if len(dateArray)>0:
	for i in range(0,len(dateArray)):
		datestoUpload.append(str(dateArray[i]["previousDate"]))
else:
	#print(len(dateArray))
	dateNow = time.strftime("%Y-%m-%d",time.localtime())
	sql= "INSERT IGNORE INTO date4FTP(previousDate) VALUES('%s')"%(dateNow)
	cursor.execute(sql)
	db.commit()
	print("You will have to wait at least 24 hours to gather enough information, be patient")
	exit(0)
#print(datestoUpload)

try:
	deleteCSVs = os.popen("rm /var/lib/mysql/ICILocal/*.csv")
	print(deleteCSVs.read())
except:
	pass
dateNow = time.strftime("%Y-%m-%d",time.localtime())

for apple in datestoUpload:
	fileName=hostname+"-"+apple+".csv"

	dataBaseArray=['Reg5MinReadings','Reg5MinReadingsW', 'Reg5MinReadingsG']
	lastDayOfData=[]

	#db.query("SELECT MAX(TimeStamping) FROM 'Reg5MinRea WHERE DATE(TimeStamping)=%s"%(a,previousDate))
	#r=db.store_result()
	#results.append(r.fetch_row(0,1))
	for a in dataBaseArray:
		db.query("SELECT MAX(DATE(TimeStamping)) AS %a FROM %s WHERE DATE(TimeStamping)='%s'"%(a,a,apple))
		r=db.store_result()
		lastDayOfData.append(r.fetch_row(0,1))

	#print(lastDayOfData[0][0])

	lastDayE=lastDayOfData[0][0]["Reg5MinReadings"]
	lastDayW=lastDayOfData[1][0]["Reg5MinReadingsW"]
	lastDayG=lastDayOfData[2][0]["Reg5MinReadingsG"]

	if lastDayE != None or lastDayW != None or lastDayG != None:
		fileCreated=CreateCSVs(lastDayE,lastDayW,lastDayG,fileName)
		try:
			uploadTheFile(fileCreated,apple)
			print(apple)
			sql= "UPDATE date4FTP SET dataUpload='yes' WHERE previousDate='%s'"%(apple)
			cursor.execute(sql)
			db.commit()
		except Exception as e:
			print(e)
			sql= "UPDATE date4FTP SET dataUpload='no' WHERE previousDate='%s'"%(apple)
			cursor.execute(sql)
			db.commit()
		#lets just store the date for next time. There is data, but remote server does not work or connection is dead
		#the reason for the try is just to avoid a break if the date is repeated
		dateNow = time.strftime("%Y-%m-%d",time.localtime())
		sql= "INSERT IGNORE INTO date4FTP(previousDate) VALUES('%s')"%(dateNow)
		cursor.execute(sql)
		db.commit()
		
	else:
		print("There is no data in database for",apple)
		print("Maybe its wise just to wait until data gathers or to check serial comms")
		try:
			#lets just store the date for next time, as there is no data im going to store it with NULL
			dateNow = time.strftime("%Y-%m-%d",time.localtime())
			sql= "INSERT INTO date4FTP(previousDate) VALUES('%s')"%(dateNow)
			cursor.execute(sql)
			db.commit()
		except:
			pass
