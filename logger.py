from influxdb import InfluxDBClient
import datetime
import json
import os
import time
import errno

class Logger():
    def __init__(self, name):
        self.name = name
        self.sensors = []     
        self.data = []
    
    def add_sensor(self, sensor_object):
        self.sensors.append(sensor_object)
    
    def connect(self, url, port, username, pwd, db_name, missed_dir = ""):
        try:
            self.client = InfluxDBClient(url, port, username, pwd, db_name)
            self.missed_dir = missed_dir
        except Exception as e:
            print(e)
    
    def upload(self):
        try:
            self.client.write_points(self.data)
        except Exception as e:
            print(e)
            try:
                os.makedirs(self.missed_dir)
            except OSError as err:
                if err.errno != errno.EEXIST:
                    raise
            file_name = "{}-missed.json".format(time.time())
            save_path = os.path.join(self.missed_dir, file_name)
            if self.missed_dir:
                with open(save_path,"w") as outfile:
                    json.dump(self.data, outfile)
                    
    def generate_body(self):
        current_time = str(datetime.datetime.utcnow())
        
        self.data_body = { "measurement": "{}".format(self.name),
            "time": current_time,
            }
    	
        for sensor in self.sensors:
            if isinstance(sensor.data , str):
                self.data_body["fields"] = { sensor.name: sensor.data }
                
            if isinstance(sensor.data, dict):
                for channel in sensor.data:
                    channel_name = "{} {}".format(sensor.name, channel)
                    self.data_body["fields"][channel_name] = sensor.data[channel]
                    
        self.data.append(self.data_body) 
           
        return self.data_body
    
    def upload_backup(self):
        try:
            for file in os.listdir(self.missed_dir):
                if file.endswith('-missed.json'):
                    fullPath = os.path.join(self.missed_dir, file)
                    with open(fullPath, 'r') as loadfile:
                        loaddata = json.load(loadfile)
                        self.data += loaddata
                    os.remove(fullPath)
        except Exception as e:
        	print(e)
        
        if self.data:
            try:
                self.upload()
            except Exception as e:
                print(e)