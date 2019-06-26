import paho.mqtt.client as mqtt
print('loucura')
mqttc = mqtt.Client("python_pub")
#mqttc.username_pw_set("psd:vwcm","vwcm")
mqttc.username_pw_set("4T1MoMmmlgd81Vj20vAe")
mqttc.connect("localhost", 1883)
mqttc.publish("v1/devices/me/telemetry", '{"temperatura": 122, "umidade": 121221}')
print('ui')
mqttc.loop(2) #timeout = 2s