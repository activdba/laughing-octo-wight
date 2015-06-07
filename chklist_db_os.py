#!/usr/bin/env python

import xlsxwriter
import time
import os
from datetime import datetime, timedelta
import re
from Crypto.pct_warnings import PowmInsecureWarning
import warnings
warnings.simplefilter("ignore", PowmInsecureWarning)
import paramiko
import json
os.environ['ORACLE_HOME'] = '/root/oracle/instantclient_11_2'
os.environ['LD_LIBRARY_PATH'] = '/root/oracle/instantclient_11_2'
import cx_Oracle

# Read JSON file to gather all connection details
with open('reqinfo.json') as data_file:
	data = json.load(data_file)

print ("EOD BOD Checklist Started")

dtntm = time.strftime("%d_%m_%Y_%H_%M_%S")
filepath = "/root/dailydbreports/"
filename = "DB_CHKLIST_"
extn = ".xlsx"
print(dtntm)
dbdt = time.strftime("%d-%b-%y")
dbdt = dbdt.upper()

yesterday = datetime.now() - timedelta(days=1)
dpbkpdt = yesterday.strftime('%d-%b-%y')
dpbkpdt = dpbkpdt.upper()
print(dpbkpdt)

# Create a workbook and add a worksheet.

#workbook = xlsxwriter.Workbook(filepath + filename + dtntm + extn)

workbook = xlsxwriter.Workbook('DBInfo.xlsx')
workbook.set_properties({'title':'BOD and EOD database checklist','subject':'BOD and EOD database checklist','author':'Nitin Chauhan','comments': 'Created with Python and XlsxWriter'})
merge_format = workbook.add_format({'bold': 1,'border': 2,'align': 'center','valign': 'vcenter','font_color': 'blue','font_name': 'Times New Roman','font_size': 10})
merge_format_bkp_green = workbook.add_format({'bold': 1,'border': 2,'align': 'center','valign': 'vcenter','font_color': 'green','font_name': 'Times New Roman','font_size': 10})
merge_format_bkp_red = workbook.add_format({'bold': 1,'border': 2,'align': 'center','valign': 'vcenter','font_color': 'red','font_name': 'Times New Roman','font_size': 10})
merge_format2 = workbook.add_format({'bold': 1,'border': 2,'align': 'left','valign': 'vcenter','font_color': 'blue','font_name': 'Times New Roman','font_size': 10})
att_format = workbook.add_format({'bold': True,'border': 2,'align': 'left','valign': 'vcenter','font_color': 'black','font_name': 'Times New Roman','font_size': 10})
att_format2 = workbook.add_format({'bold': True,'border': 2,'align': 'center','valign': 'vcenter','font_color': 'black','font_name': 'Times New Roman','font_size': 10})
att_out_format = workbook.add_format({'border': 1,'align': 'right','valign': 'vcenter','font_color': 'black','font_name': 'Times New Roman','font_size': 10})
att_out_format2 = workbook.add_format({'border': 1,'align': 'left','valign': 'vcenter','font_color': 'black','font_name': 'Times New Roman','font_size': 10})
date_format = workbook.add_format({'num_format': 'd mmmm yyyy','font_name': 'Times New Roman','font_size': 10,'border': 1,'align': 'right','valign': 'vcenter','font_color': 'black'})
date_format_bkp = workbook.add_format({'num_format': 'dd mmm yyyy','font_name': 'Times New Roman','font_size': 10,'border': 1,'align': 'right','valign': 'vcenter','font_color': 'black'})
bkp_format_green = workbook.add_format({'border': 1,'align': 'right','valign': 'vcenter','font_color': 'green','font_name': 'Times New Roman','font_size': 10})
bkp_format_red = workbook.add_format({'border': 1,'align': 'right','valign': 'vcenter','font_color': 'red','font_name': 'Times New Roman','font_size': 10})


#queries

dbname="SELECT name FROM v$database"
omode="select open_mode from v$database"
dbid="select dbid from v$database"
cdate="select created from v$database"
logmode="select log_mode from v$database"
hostname="select host_name from v$instance"
version="select version from v$instance"
sga="select value/1024/1024 from v$parameter where name='sga_target'"
pga="select value/1024/1024 from v$parameter where name='pga_aggregate_target'"
psize="select (select sum(bytes)/1024/1024/1024 from dba_data_files) + (select nvl(sum(bytes),0)/1024/1024/1024 from dba_temp_files)+(select sum(bytes)/1024/1024/1024 from sys.v_$log) from dual"
lsize="select sum(bytes/1024/1024/1024) from dba_segments"
stime="select round(sysdate - STARTUP_TIME) from v$instance"

sratio="select (1-d.VALUE/m.value)*100 FROM V$sysstat d,v$sysstat m WHERE d.name ='sorts (disk)' and m.name='sorts (memory)'"
dratio="select (1-(sum(getmisses)/sum(gets)))*100 from v$rowcache"
lratio="select sum(gethitratio)/count(*) *100 from v$librarycache"
bratio="SELECT (1-PHY.VALUE/(cur.value+con.value))*100 from v$sysstat phy,v$sysstat con,v$sysstat cur where cur.name='db block gets' and con.name='consistent gets' and phy.name='physical reads'"
iratio="SELECT (1-l.VALUE/(l.value+s.value))*100 FROM V$sysstat l,v$sysstat s WHERE s.name ='table scans (short tables)' and l.name= 'table scans (long tables)'"
rratio="select (1-(re.value/r.value ))*100 from v$sysstat re ,v$sysstat r where re.name ='redo buffer allocation retries' and r.name='redo entries'"
dbtrans="select VALUE from v$sysstat where name='user commits'"

tabname="select distinct tablespace_name from dba_data_files order by tablespace_name"
tabtotal="select sum(bytes/1024/1024) from dba_data_files group by tablespace_name order by tablespace_name"
tabuse="select round((t.totalspace-nvl(fs.freespace,0)),2) from (select round(sum(d.bytes)/(1024*1024)) as totalspace,d.tablespace_name tablespace from dba_data_files d group by d.tablespace_name) t,(select round(sum(f.bytes)/(1024*1024)) as freespace,f.tablespace_name tablespace from dba_free_space f group by f.tablespace_name) fs where t.tablespace=fs.tablespace (+) order by t.tablespace"
tabavail="select nvl(fs.freespace,0) from (select round(sum(d.bytes)/(1024*1024)) as totalspace,d.tablespace_name tablespace from dba_data_files d group by d.tablespace_name) t,(select round(sum(f.bytes)/(1024*1024)) as freespace,f.tablespace_name tablespace from dba_free_space f group by f.tablespace_name) fs where t.tablespace=fs.tablespace (+) order by t.tablespace"

prseq="select max(sequence#) from v$archived_log"
prarc="select archived from v$archived_log where registrar='LGWR' or registrar='ARCH' and sequence#=(select max(sequence#) from v$archived_log)"
prapp="select applied from v$archived_log where registrar='LGWR' or registrar='ARCH' and sequence#=(select max(sequence#) from v$archived_log)"

drseq="select max(sequence#) from v$archived_log"
drarc="select archived from v$archived_log where sequence#=(select max(sequence#) from v$archived_log)"
drapp="select applied from v$archived_log where sequence#=(select max(sequence#) from v$archived_log)"

rmanstat="select status from v$RMAN_BACKUP_JOB_DETAILS where END_TIME=(select max(END_TIME) from v$RMAN_BACKUP_JOB_DETAILS)"
dpbkpstat="select * from dp_bkp_status where bkp_date='%s' order by SCHEMA_NAME"%(dpbkpdt)

dbparameters = [dbname,omode,dbid,cdate,logmode,hostname,version,sga,pga,psize,lsize,stime]
dbratios = [sratio,dratio,lratio,bratio,iratio,rratio,dbtrans]
dbtabs = [tabname,tabtotal,tabuse,tabavail]
prsyncs = [prseq,prarc,prapp]
drsyncs = [drseq,drarc,drapp]


uid = "system"
host = data["DBPRICONNDET"]["HOST"]
port = data["DBPRICONNDET"]["PORT"]

duid = "sys"
drhost = data["DBDRCONNDET"]["HOST"]
drport = data["DBDRCONNDET"]["PORT"]

output_json = json.load(open('reqinfo.json'))
for majorkey, subdict in output_json.items():
	if majorkey == 'DBPRIINFO':
		for dbname, passwd in subdict.items():
			connection = cx_Oracle.connect(uid + "/" + passwd + "@" + host + ":" + port + "/" + dbname)
			cursor = connection.cursor()

# Create worksheet
			dbrow=1
			rrow=16
			trow=28
			tcol=-1
			syncol=4
			drcol=4

			worksheet = workbook.add_worksheet(dbname)
			
			worksheet.set_column('A:A', 25)
			worksheet.set_column('B:B', 18)
			worksheet.set_column('C:C', 18)
			worksheet.set_column('D:D', 19)
			worksheet.set_column('E:E', 10)
			worksheet.set_column('F:F', 15)
			worksheet.set_column('G:G', 15)
			worksheet.set_column('H:H', 25)
			worksheet.set_column('I:I', 15)
			worksheet.set_column('K:K', 25)
			
			now = time.strftime("%c")
			## date and time representation
			#print ("Current date & time " + time.strftime("%c"))
			
			worksheet.merge_range('A1:B1', now, merge_format)
			worksheet.merge_range('A15:B15','PERFORMANCE INFORMATION', merge_format)
			worksheet.merge_range('A25:D25','TABLESPACE INFORMATION', merge_format)
			worksheet.merge_range('D6:I6','DAILY BACKUP INFORMATION',merge_format)
			worksheet.merge_range('D7:E7','RMAN BACKUP STATUS :',merge_format2)
			worksheet.merge_range('D8:E8','DATAPUMP BACKUP STATUS :',merge_format2)
			worksheet.merge_range('D1:H1','DR SYNC STATUS',merge_format)
			worksheet.merge_range('D2:D4','SYNC STATUS',merge_format)
			worksheet.merge_range('F16:K16','MOUNTPOINT SPACE INFORMATION',merge_format)
			
			worksheet.write(1, 0, 'DATABASE NAME :',att_format)
			worksheet.write(2, 0, 'OPEN MODE :',att_format)
			worksheet.write(3, 0, 'DATABASE ID :',att_format)
			worksheet.write(4, 0, 'CREATED :',att_format)
			worksheet.write(5, 0, 'LOG MODE :',att_format)
			worksheet.write(6, 0, 'HOSTNAME :',att_format)
			worksheet.write(7, 0, 'VERSION :',att_format)
			worksheet.write(8, 0, 'SGA SIZE (MB) :',att_format)
			worksheet.write(9, 0, 'PGA SIZE (MB) :',att_format)
			worksheet.write(10, 0, 'PHYSICAL SIZE (GB) :',att_format)
			worksheet.write(11, 0, 'LOGICAL SIZE (GB) :',att_format)
			worksheet.write(12, 0, 'UPTIME (DAYS) :',att_format)
			
			worksheet.write(16, 0, 'SORT RATIO :',att_format)
			worksheet.write(17, 0, 'DICTIONARY HIT RATIO :',att_format)
			worksheet.write(18, 0, 'LIBRARY CACHE HIT RATIO :',att_format)
			worksheet.write(19, 0, 'BUFFER CACHE HIT RATIO :',att_format)
			worksheet.write(20, 0, 'INDEX LOOKUP RATIO :',att_format)
			worksheet.write(21, 0, 'REDO LOG HIT RATIO :',att_format)
			worksheet.write(22, 0, 'NUMBER OF TRANSACTIONS :',att_format)
			
			worksheet.write(26, 0, 'TABLESPACE NAME',att_format2)
			worksheet.write(26, 1, 'TOTAL SIZE(MB)',att_format2)
			worksheet.write(26, 2, 'USED SPACE(MB)',att_format2)
			worksheet.write(26, 3, 'AVAILABLE SPACE(MB)',att_format2)
			
			worksheet.write(1, 4, 'DATABASE',att_format)
			worksheet.write(1, 5, 'SEQUENCE #',att_format)
			worksheet.write(1, 6, 'ARCHIVED',att_format)
			worksheet.write(1, 7, 'APPLIED',att_format)
			worksheet.write(2, 4, 'PRIMARY :',att_format)
			worksheet.write(3, 4, 'STANDBY :',att_format)
			
			worksheet.write('F8', 'BACKUP DATE',att_format)
			worksheet.write('G8', 'CLIENT',att_format)
			worksheet.write('H8', 'SCHEMA_NAME',att_format)
			worksheet.write('I8', 'STATUS',att_format)
			
			worksheet.write('F17', 'FILESYSTEM',att_format)
			worksheet.write('G17', 'TOTAL SIZE(GB)',att_format)
			worksheet.write('H17', 'USED(GB)',att_format)
			worksheet.write('I17', 'FREE(GB)',att_format)
			worksheet.write('J17', 'USED(%)',att_format)
			worksheet.write('K17', 'MOUNTED ON',att_format)
			
			# ways to connect 
			# 1st way
			#connection_String = 'system/infotech@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.99.38)(PORT=3128)))(CONNECT_DATA=(SID=servname)))'
			#connection = cx_Oracle.connect(connection_String)
			
			# 2nd way
			#connection = cx_Oracle.connect('system/infotech@192.168.99.38:3128/servname')
			
			# 3rd way
	
	#for majorkey, subdict in output_json.items():
	# if majorkey == servname:
	#  for dbnam, passwd in subdict.items():
	#   dpwd = passwd
	#   drconnection = cx_Oracle.connect(druid + "/" + drpwd + "@" + drhost + ":" + drport + "/" + drname, mode = cx_Oracle.SYSDBA)
	#   drcursor = drconnection.cursor()
	
			for dbparameter in dbparameters:
				if 'created' in dbparameter:
					cursor.execute(dbparameter)
					for ro in cursor.fetchone():
						print("i am executing in db parameteres")
						worksheet.write_datetime(4, 1, ro,date_format)
						dbrow=dbrow+1
				else:
					cursor.execute(dbparameter)
					for ro in cursor.fetchone():
						worksheet.write(dbrow, 1, ro,att_out_format)
						dbrow=dbrow+1
	
			for dbratio in dbratios:
				cursor.execute(dbratio)
				for ro in cursor.fetchone():
					worksheet.write(rrow, 1, ro,att_out_format)
					rrow=rrow+1
	
			for dbtab in dbtabs:
				if 'distinct' in dbtab:
					cursor.execute(dbtab)
					tcol=tcol+1
					trow=28
					for ro in cursor.fetchall():
						worksheet.write(trow, tcol, ro[0],att_format)
						trow=trow+1
				else:
					cursor.execute(dbtab)
					tcol=tcol+1
					trow=28
					for ro in cursor.fetchall():
						worksheet.write(trow, tcol, ro[0],att_out_format)
						trow=trow+1
	
			for prsync in prsyncs:
				syncol=syncol+1
				cursor.execute(prsync)
				for ro in cursor.fetchone():
					worksheet.write(2,syncol,ro,att_out_format) 
	
		#	for drsync in drsyncs:
		#		drcol=drcol+1
		#		drcursor.execute(drsync)
		#		for ro in drcursor.fetchone():
		#			worksheet.write(3,drcol,ro,att_out_format)
	
			cursor.execute(rmanstat)
			for ro in cursor.fetchone():
				if ro == 'COMPLETED':
					worksheet.merge_range('F7:I7',ro,merge_format_bkp_green)
				else:
					worksheet.merge_range('F7:I7',ro,merge_format_bkp_red)
	
			cursor.execute(dpbkpstat)
			bcol=5
			brow=8
			for ro in cursor.fetchall():
				worksheet.write_datetime(brow, 5, ro[0],date_format_bkp)
				worksheet.write(brow, 6, ro[1],att_out_format)
				worksheet.write(brow, 7, ro[2],att_out_format)
				if ro[3] == 'COMPLETED':
					worksheet.write(brow, 8, ro[3],bkp_format_green)
				else:
					worksheet.write(brow, 8, ro[3],bkp_format_red)
				brow=brow+1
	
			cursor.close()
			connection.close()

workbook.close()
print ("EOD BOD Checklist completed")
