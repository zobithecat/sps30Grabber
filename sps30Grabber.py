import sqlite3
from time import sleep
from sensirion_sps30 import SPS30

headers = ["pm1.0", "pm2.5", "pm4.0", "pm10.0", "nc0.5", "nc1.0", "nc2.5", "nc4.5", "nc10.0", "typical particle size"]

port: str = "/dev/cu.usbserial-AB0M32KP"
sps30 = SPS30(port)

sps30.start_measurement()
sleep(5)

data = sps30.read_values()

conn = sqlite3.connect('sps30.db')
cur = conn.cursor()
conn.execute("CREATE TABLE IF NOT EXISTS sps30(id INTEGER PRIMARY KEY AUTOINCREMENT, pm1_0 REAL, pm2_5 REAL, pm4_0 REAL, pm10_0 REAL, nc0_5 REAL, nc1_0 REAL, nc2_5 REAL, nc4_5 REAL, nc10_0 REAL, typical_particle_size REAL, timestamp DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))")
conn.commit()
conn.execute("INSERT INTO sps30 (pm1_0, pm2_5, pm4_0, pm10_0, nc0_5, nc1_0, nc2_5, nc4_5, nc10_0, typical_particle_size) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
conn.commit()
conn.close()
    
for i in range(len(headers)):
    print(f"{headers[i]}: {data[i]}")

sps30.stop_measurement()
