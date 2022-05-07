from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from bson.objectid import ObjectId
from kafka.structs import TopicPartition

client = MongoClient("mongodb+srv://khaled:1234@cluster0.bdpuf.mongodb.net/mydb?retryWrites=true&w=majority")
collection = client.mydb.drone
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], enable_auto_commit= False)
consumer.subscribe(['k_connectin_status','k_armed_status',
    'k_flight_mode','k_velocity','k_altitude','k_gps_status','k_lat','k_long'])

objid= "62645d42e0fd10a199001d5e"
while True:
    
    # poll messages each certain ms
    raw_messages = consumer.poll(timeout_ms=10000)
   # print(raw_messages)
    #print(raw_messages.values())
    # for each messages batch
    for topic_partition, messages in raw_messages.items():
        # if message topic is k_connectin_status

        #connection status
        if topic_partition.topic == 'k_connectin_status':
            for msg in messages: 
                connection_result = str(msg.value,'UTF-8')
                break
            if connection_result =="True":
                tvalue= True
            else:
                tvalue= False
            drone = collection.find_one({"_id": ObjectId(objid)})
            if drone:
                collection.update_one({"_id": ObjectId(objid)},{'$set':{'connected':tvalue}})  
                print('connect updated on the Database with value = ',tvalue)
                 
        #armed status
        if topic_partition.topic == 'k_armed_status':
            for msg in messages: 
                armed_result = str(msg.value,'UTF-8')
                
            if armed_result =="True":
                fvalue= True
            else:
                fvalue= False
            drone = collection.find_one({"_id": ObjectId(objid)})
            if drone:
                collection.update_one({"_id": ObjectId(objid)},{'$set':{'armed':fvalue}})  
                print('armed updated on the Database with value = ',fvalue)
                
        #speed
        if topic_partition.topic == 'k_velocity':
            for msg in messages: 
                velocity = str(msg.value,'UTF-8')
                
            drone = collection.find_one({"_id": ObjectId(objid)})
            if drone:
                collection.update_one({"_id": ObjectId(objid)},{'$set':{'speed':float(velocity)}})  
                print('speed updated on the Database with value = ',velocity)
                
        #alltitdue
        if topic_partition.topic == 'k_altitude':
            for msg in messages: 
                alltitdue_result = str(msg.value,'UTF-8')
                
            drone = collection.find_one({"_id": ObjectId(objid)})
            if drone:
                collection.update_one({"_id": ObjectId(objid)},{'$set':{'altitude':float(alltitdue_result)}})  
                print('alltitude updated on the Database with value = ',alltitdue_result)
                
        #gps_status
        if topic_partition.topic == 'k_gps_status':
            for msg in messages: 
                status_result = str(msg.value,'UTF-8')
                
            drone = collection.find_one({"_id": ObjectId(objid)})
            if drone:
                collection.update_one({"_id": ObjectId(objid)},{'$set':{'gps_status':int(status_result)}})  
                print('gps_status  updated on the Database with value = ',status_result)
                
        #flight mode
        if topic_partition.topic == 'k_flight_mode':
            for msg in messages: 
                mode_result = str(msg.value, 'UTF-8')

            drone = collection.find_one({"_id": ObjectId(objid)})
            if drone:
                collection.update_one({"_id": ObjectId(objid)},{'$set':{'flight_mode':mode_result}})  
                print('flight_mode updated on the Database with value = ',mode_result)
                
        if topic_partition.topic == 'k_lat':
            for lat in messages: 
                latresult = float(lat.value)
        if topic_partition.topic=='k_long':
            for longg in messages:
                inner = float(longg.value)
            drone = collection.find_one({"_id": ObjectId(objid)})
            if drone:
                collection.update_one({"_id": ObjectId(objid)},{'$set':{'coordinates':[inner,latresult]}})  
                print('cordinates updated on the Database with value = ',latresult,' , ', inner) 
                 





#coordinates array[ dec#,dec#] two diffrent topics 
#connected on boolean
#armed boolean
#speed dec# @done
#alltitdue dec# @done
#gps_status int# @done
#flight_mode string @done
#62645d42e0fd10a199001d5e


#30 44
#alt 56
#gps status -1
#flight mode maunal
#speed 11
#armed true
#connected true