import sys
import matplotlib.pyplot as plt
import pymysql
import matplotlib.animation as animation
#
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
date = []
close = []
#
def plot_fun(i, date, close):
    con = pymysql.connect(host='127.0.0.1', user='root', passwd='greatnaolAT0*', db='stock')
    cursor = con.cursor()
    cursor.execute(
        "select CAST(date as datetime), CAST(close AS UNSIGNED), CAST(high AS UNSIGNED), CAST(volume AS UNSIGNED) from msft_stock WHERE date > '2018/01/01' order by date");
    #
    result = cursor.fetchall()
    date = []
    close = []
    for record in result:
        date.append(record[0])
        close.append(record[1])
#        high.append(record[2])
#        low.append(record[3])
#        volume.append(record[4])
#
#    fig = plt.figure(figsize=(12, 7))
#
    ax.clear()
    ax.plot(date, close)
    #plt.plot(date, close, 'ro')
    plt.title('Time series data for closing price', fontsize=20)
    plt.xlabel(' Date ', fontsize=14)
    plt.ylabel('Closing Price($)', fontsize=14)
    plt.savefig('date_close_msft.jpg')
#
ani = animation.FuncAnimation(fig, plot_fun, fargs=(date, close), interval=1000)
plt.show()
