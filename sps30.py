"""
sg_yoo, Sep 14, 2023
- A fork from Binh Ng (2020)
https://github.com/binh-bk/Sensirion_SPS30
"""
import serial, struct, time
import subprocess
from operator import invert
import os
import json
import csv
import paho.mqtt.client as mqtt
import sqlite3
import ssl

# set localtime
os.environ["TZ"] = "Asia/Seoul"
time.tzset()
SPS_PORT = "/dev/ttyS0"
ARDUINO_PORT = "/dev/ttyACM0"

# MQTT host, users
ENDPOINT = "a33hnbvehzvvml-ats.iot.ap-northeast-2.amazonaws.com"
THING_NAME = "basicPubSub"
CERTPATH = "./cert/rpi_4_01.cert.pem"  # cert파일 경로
KEYPATH = "./cert/rpi_4_01.private.key"  # key 파일 경로
CAROOTPATH = "./cert/root-CA.crt"  # RootCaPem 파일 경로
TOPIC = "sdk/test/python"  # 주제
TOPIC_ALIAS_MAX = 60


class SPS30GRABBER:
    NAME = "SPS30"
    WARMUP = 20  # seconds

    def __init__(
        self,
        deviceId,
        spsPort,
        arduinoPort,
        save_data=True,
        push_mqtt=False,
        INTERVAL=60,
    ):
        self.deviceId = deviceId
        self.spsPort = spsPort
        self.arduinoPort = arduinoPort
        self.interval = INTERVAL
        self.warmup = SPS30GRABBER.WARMUP
        self.save_data = save_data
        self.push_mqtt = push_mqtt
        self.name = SPS30GRABBER.NAME
        self.lastSample = 0
        self.fanOn = 0
        self.is_started = False
        self.ser = serial.Serial(
            self.spsPort, baudrate=115200, stopbits=1, parity="N", timeout=2
        )
        self.arduinoSer = serial.Serial(
            self.arduinoPort, baudrate=115200, stopbits=1, parity="N", timeout=2
        )
        self.headers = [
            "datetime",
            "pm1.0",
            "pm2.5",
            "pm4.0",
            "pm10.0",
            "nc0.5",
            "nc1.0",
            "nc2.5",
            "nc4.5",
            "nc10.0",
            "typical particle size",
        ]
        self.dataDict = {
            "datetime": "",
            "device_id": self.deviceId,
            "pm1.0": 0.0,
            "pm2.5": 0.0,
            "pm4.0": 0.0,
            "pm10.0": 0.0,
            "nc0.5": 0.0,
            "nc1.0": 0.0,
            "nc2.5": 0.0,
            "nc4.5": 0.0,
            "nc10.0": 0.0,
            "typical particle size": 0.0,
            "temp": 0.0,
            "humidity": 0.0,
            "co2level": 0.0,
        }
        self.startByte = [0x7E, 0x00, 0x00, 0x02, 0x01, 0x03, 0xF9, 0x7E]
        self.stopByte = [0x7E, 0x00, 0x01, 0x00, 0xFE, 0x7E]
        self.askByte = [0x7E, 0x00, 0x03, 0x00, 0xFC, 0x7E]
        self.mqttClient = self.initMqtt()

    def on_connect(self, mqttc, userdata, flags, rc, properties=None):
        print("hello, world! mqtt is connected!")
        mqttc.is_connected = True

    def initMqtt(self):
        mqtt_client = mqtt.Client(client_id=THING_NAME)
        mqtt_client.on_connect = self.on_connect
        mqtt_client.tls_set(
            CAROOTPATH,
            certfile=CERTPATH,
            keyfile=KEYPATH,
            tls_version=ssl.PROTOCOL_TLSv1_2,
            ciphers=None,
        )
        mqtt_client.connect(ENDPOINT, port=8883)
        mqtt_client.loop_start()
        print("mqtt is initialized!")
        return mqtt_client

    def get_usb(self):
        try:
            with subprocess.Popen(
                ["ls /dev/ttyS* /dev/ttyACM0"], shell=True, stdout=subprocess.PIPE
            ) as f:
                usbs = f.stdout.read().decode("utf-8")
            usbs = usbs.split("\n")
            usbs = [usb for usb in usbs if len(usb) > 3]
        except Exception as e:
            print("No USB available")
        return usbs

    def time_(self):
        return int(time.time())

    def datetime_(self):
        return time.strftime("%x %X", time.localtime())

    def host_folder(self):
        csv_save_dir = "csv"
        basedir = os.path.abspath(os.path.dirname(__file__))
        basedir = basedir + "/" + csv_save_dir
        this_month_folder = time.strftime("%b%Y")
        all_dirs = [
            d for d in os.listdir(basedir) if os.path.isdir(os.path.join(basedir, d))
        ]
        if len(all_dirs) == 0 or this_month_folder not in all_dirs:
            os.makedirs(basedir + "/" + this_month_folder)
            print("created: {}".format(this_month_folder))
        return os.path.join(basedir, this_month_folder)

    def record_data(self):
        data = self.dataDict
        print("record data: ", data)
        # id_ = data.split(",")[0]
        # id_ = f"{id_}"
        id_ = self.name
        filename = os.path.join(self.host_folder(), f"{id_}.csv")
        with open(filename, "a+") as f:
            w = csv.writer(f)
            w.writerow(data.values())
            print(data)
        return None

    def __str__(self):
        return f"{self.port}, {self.name}, {self.fanOn}, {self.lastSample}"

    def push_mqtt_server(self):
        data = self.dataDict
        print(f"Process for MQTT {data}")
        payload = data
        print(f"MQTT: {payload}")
        payload = json.dumps(payload)
        if self.mqttClient.is_connected:
            try:
                self.mqttClient.publish(TOPIC, payload)
            except Exception as e:
                print("Error: {}".format(e))
                pass
        return None

    def start(self):
        self.ser.write(self.startByte)

    def stop(self):
        self.ser.write(self.stopByte)

    def read_arduino_values(self):
        print("hyello! arduino!")
        self.arduinoSer.flushInput()
        dataList = []
        if self.arduinoSer.readable():
            for i in range(3):
                response = self.arduinoSer.readline()
                dataList.append(response[: len(response) - 2].decode())
        print("response:", dataList)
        return dataList

    def read_sps30_values(self):
        self.ser.flushInput()
        # Ask for data
        self.ser.write(self.askByte)
        toRead = self.ser.inWaiting()
        # Wait for full response
        # (may be changed for looking for the stop byte 0x7E)
        while toRead < 47:
            toRead = self.ser.inWaiting()
            print(f"Wait: {toRead}")
            time.sleep(1)
        raw = self.ser.read(toRead)

        # Reverse byte-stuffing
        if b"\x7D\x5E" in raw:
            raw = raw.replace(b"\x7D\x5E", b"\x7E")
        if b"\x7D\x5D" in raw:
            raw = raw.replace(b"\x7D\x5D", b"\x7D")
        if b"\x7D\x31" in raw:
            raw = raw.replace(b"\x7D\x31", b"\x11")
        if b"\x7D\x33" in raw:
            raw = raw.replace(b"\x7D\x33", b"\x13")

        # Discard header and tail
        rawData = raw[5:-2]

        try:
            data = struct.unpack(">ffffffffff", rawData)
        except struct.error:
            data = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

        return data

    def read_sps30_serial_number(self):
        self.ser.flushInput()
        self.ser.write([0x7E, 0x00, 0xD0, 0x01, 0x03, 0x2B, 0x7E])
        toRead = self.ser.inWaiting()
        while toRead < 7:  # 24
            toRead = self.ser.inWaiting()
            print(f"Wait: {toRead}")
            time.sleep(1)
        raw = self.ser.read(toRead)

        # Reverse byte-stuffing
        if b"\x7D\x5E" in raw:
            raw = raw.replace(b"\x7D\x5E", b"\x7E")
        if b"\x7D\x5D" in raw:
            raw = raw.replace(b"\x7D\x5D", b"\x7D")
        if b"\x7D\x31" in raw:
            raw = raw.replace(b"\x7D\x31", b"\x11")
        if b"\x7D\x33" in raw:
            raw = raw.replace(b"\x7D\x33", b"\x13")

        # Discard header, tail and decode
        serial_number = raw[5:-3].decode("ascii")
        return serial_number

    def save_data_to_sqlite(self):
        data = list(self.dataDict.values())
        # print("dataDict:", self.dataDict)
        # print("data values: ", data)
        conn = sqlite3.connect("./sqliteDB/sps30.db")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS 
            sps30(id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id STRING,
            pm1_0 REAL, 
            pm2_5 REAL, 
            pm4_0 REAL, 
            pm10_0 REAL, 
            nc0_5 REAL, 
            nc1_0 REAL, 
            nc2_5 REAL, 
            nc4_5 REAL, 
            nc10_0 REAL, 
            typical_particle_size REAL,
            temp REAL,
            humidity REAL,
            co2level REAL,  
            datetime STRING)
            """
        )
        conn.commit()
        conn.execute(
            """INSERT INTO 
            sps30 
            (datetime,
            device_id,
            pm1_0,
            pm2_5, 
            pm4_0, 
            pm10_0, 
            nc0_5, 
            nc1_0, 
            nc2_5, 
            nc4_5, 
            nc10_0, 
            typical_particle_size,
            temp,
            humidity,
            co2level) 
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            data,
        )
        conn.commit()
        conn.close()

    def updateSps30Data(self, data):
        dataList = list(data)
        dataList.insert(0, self.datetime_())
        for idx, key in enumerate(self.headers):
            self.dataDict[key] = dataList[idx]

    def updateArduinoData(self, data):
        for value in data:
            if value[:1] == "C":
                self.dataDict["co2level"] = float(value[1:])
            if value[:1] == "T":
                self.dataDict["temp"] = float(value[1:])
            if value[:1] == "H":
                self.dataDict["humidity"] = float(value[1:])

    def run_query(self):
        if self.time_() - self.lastSample >= self.interval:
            if not self.is_started:
                self.start()
                self.fanOn = self.time_()
                self.is_started = True
            if self.name == SPS30GRABBER.NAME:
                name_ = self.read_sps30_serial_number()
                if len(name_) > 0:
                    self.name = f"SPS_{name_}"
            if self.time_() - self.fanOn >= self.warmup:
                output = self.read_sps30_values()
                data = self.read_arduino_values()
                self.updateSps30Data(output)
                self.updateArduinoData(data)
                print("dataDict: ", self.dataDict)
                # sensorData = ""
                # for val in output:
                #     sensorData += "{0:.2f},".format(val)

                # output = ",".join([self.name, self.datetime_(), sensorData[:-1]])
                self.save_data_to_sqlite()
                self.lastSample = self.time_()
                if self.is_started:
                    self.stop()
                    self.is_started = False
                if self.save_data:
                    self.record_data()
                if self.push_mqtt:
                    self.push_mqtt_server()
            else:
                time.sleep(1)
        return None

    def close_port(self):
        self.ser.close()
        self.arduinoSer.close()


if __name__ == "__main__":
    sps30Grabber = SPS30GRABBER(
        deviceId="scilab-01",
        spsPort=SPS_PORT,
        arduinoPort=ARDUINO_PORT,
        push_mqtt=True,
        INTERVAL=36,
    )
    while True:
        sps30Grabber.run_query()
