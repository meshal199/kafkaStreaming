import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from json import dumps
import time
import random 
import math

#client_id = f'python-mqtt-{random.randint(0, 1000)}'
client = mqtt.Client()

broker_address = 'cf7191ce-internet-facing-18f111026a539446.elb.ap-south-1.amazonaws.com'  # Broker address
client.username_pw_set(username="Edge", password="1234")
port = 1883  # Broker port

client.connect(broker_address, port) 



kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    

def on_message(client, userdata, message):
	start = time.time()
	#connected
	if message.topic == 'Connection_Status':
		msg_payload = (message.payload)
		data = msg_payload
		kafka_producer.send('k_connectin_status', msg_payload)
		print('KAFKA: jsut publish ',data , message.topic)
	#armed
	if message.topic == 'Armed':
		msg_payload = (message.payload)
		data = msg_payload
		kafka_producer.send('k_armed_status', msg_payload)
		print('KAFKA: jsut publish ',data , message.topic)
		
	#speed
	if message.topic == 'Velocity-x':
		msg_payload_x = (message.payload)
		data_x = msg_payload_x
		if message.topic == 'Velocity-y':
			msg_payload_y = message.payload
			data_y = msg_payload_y
			msg_payload = math.sqrt(int(pow(msg_payload_x,2))+ int(pow(msg_payload_y,2)))
			kafka_producer.send('k_velocity', msg_payload)
			print('KAFKA: jsut publish ',msg_payload , message.topic)
	#altitude
	if message.topic == 'Altitude':
		msg_payload = (message.payload)
		data = msg_payload
		kafka_producer.send('k_altitude', msg_payload)
		print('KAFKA: jsut publish ',data , message.topic)
	#gps_status
	if message.topic == 'GPS_status':
		msg_payload = (message.payload)
		data = msg_payload
		kafka_producer.send('k_gps_status', msg_payload)
		print('KAFKA: jsut publish ',data , message.topic)
	#lat
	if message.topic == 'GPS_latitude':
		msg_payload =(message.payload)
		data = msg_payload
		kafka_producer.send('k_lat', msg_payload)
		print('KAFKA: jsut publish ',data , message.topic)
	#long
	if message.topic == 'GPS_longitude':
		msg_payload = (message.payload)
		data = msg_payload
		kafka_producer.send('k_long', msg_payload)
		print('KAFKA: jsut publish ',data , message.topic)
	#flight_mode
	if message.topic == 'Flight_mode':
		msg_payload = (message.payload)
		data = msg_payload
		kafka_producer.send('k_flight_mode', msg_payload)
		print('KAFKA: jsut publish ',data , message.topic)
	end = time.time()
	#print('time take is= ',end - start)
	
#coordinates array[ dec#,dec#] two diffrent topics 
#connected on boolean
#armed boolean
#speed dec# @done
#alltitdue dec# @done
#gps_status int# @done
#flight_mode string @done


client.subscribe('GPS_latitude')
client.subscribe('Velocity-x')
client.subscribe('Velocity-y')
client.subscribe('Connection_Status')
client.subscribe('Flight_mode') 
client.subscribe('GPS_longitude')
client.subscribe('GPS_status')
client.subscribe('Altitude')
client.subscribe('Armed')
client.on_message = on_message 
print('connected to MQTT broker')
client.loop_forever()