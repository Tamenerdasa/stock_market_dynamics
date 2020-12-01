import sys
import matplotlib.pyplot as plt
import pymysql
#connect to mysql database
con = pymysql.connect(host='127.0.0.1', user='root', passwd='greatnaolAT0*', db='stock')
cursor = con.cursor()
cursor.execute("select CAST(date as datetime), CAST(close AS UNSIGNED), CAST(high AS UNSIGNED), CAST(volume AS UNSIGNED) from msft_stock WHERE date > '2018/01/01' order by date");
result = cursor.fetchall()
date  = []
volume = []
for record in result:
    date.append(record[0])
    volume.append(record[3])
fig = plt.figure(figsize=(12,5))
plt.bar(date,volume,width=15)
#fig.suptitle('Time series data for closing price', fontsize=20)
plt.xlabel(' Date ', fontsize=14)
plt.ylabel('stock volume', fontsize=14)
plt.savefig('date_volume_msft.jpg')
plt.show()
