from time import sleep

from sensirion_sps30 import SPS30

headers = ["pm1.0", "pm2.5", "pm4.0", "pm10.0", "nc0.5", "nc1.0", "nc2.5", "nc4.5", "nc10.0", "typical particle size"]

port: str = "/dev/cu.usbserial-AB0M32KP"
sps30 = SPS30(port)

sps30.start_measurement()
sleep(5)

data = sps30.read_values()
print(data)

for i in range(len(headers)):
    print(f"{headers[i]}: {data[i]}")

sps30.stop_measurement()