from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import pickle
#
KAFKA_TOPIC_NAME_CONS = "msfttopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

print("Kafka Producer Application Started ... ")

kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
						 value_serializer=lambda x: dumps(x).encode('utf-8'))

def processData(ticker):
	ts = TimeSeries('DRLVZJXZ2S4ULOCH', output_format='pandas')
	Monthly_TS_df, meta_data=ts.get_monthly_adjusted(symbol=ticker)
	#Change column names to approprate form,
	for column in Monthly_TS_df.columns:
		Monthly_TS_df.rename({column: column.split('. ')[1]}, axis=1, inplace=True)
	return Monthly_TS_df.sort_values('date')
#
df = processData('MSFT')
#
for ind in df.index:
    message = {}
 #   message['date'] = df['date'][ind]
    message['date'] = str(ind).split(' ')[0]
    message['open'] = df['open'][ind]
    message['high'] = df['high'][ind]
    message['low'] = df['low'][ind]
    message['close'] = df['close'][ind]
    message['volume'] = df['volume'][ind]
    print("Message to be sent: ", message)
    kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
    kafka_producer_obj.flush()
    time.sleep(1)
