import pcap, dpkt, binascii, timeit, time, sys#, pika
from kafka import KafkaProducer
import json
from time import sleep
from datetime import datetime
import time

# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

# Call the producer.send method with a producer-record
print("ctrl+c to stop...")

if len(sys.argv) != 2:#6:
	print >> sys.stderr, "Usage: %s [simulador.py Runtime]" % (sys.argv[0]) #[RabbitMQ host] [RabbitMQ user] [RabbitMQ password] [RabbitMQ vhost]..." % (sys.argv[0])
	sys.exit(1)
else:
	try:
		if float(sys.argv[1]) < 1:
			sys.exit(1)
		runtime = float(sys.argv[1])
	except:
		print "[runtime] Must be of type number and >= 1"
		sys.exit(1)
runtime = float(sys.argv[1])

arquivo = open('/home/rsi-psd-vm/Documents/rsi-psd-codes/psd/pratica-05/vendorMacs.prop', 'r')
dados=arquivo.read()
arquivo.close()
dados = dados.split('\n')[:-1]
dados = dict(map(lambda x: (x.split('=')[0], x.split('=')[1]), dados))
# vendors = {}

# number_of_probes = 0
# listaMAC = {}
# PNLs = {'total': 0}
# directed_probes=0
# broadcast_probes=0
# listaSSID=[]

maxPkts = 50
nPkts=0
lista, lista1 =[], []

def conteudoMap(i):
	map(lambda x: lista.append(x[0]),pcap.pcap(name='./probes/probes-2013-03-30.pcap'+str(i)))
map(conteudoMap,range(4,5))
tempoEsperado = (lista[-1]-lista[0])

print('tempoEsperado: %1.2fh'%(tempoEsperado/(runtime*60*60)))
# for i in range(7):
# 	print('probes-2013-03-30.pcap'+str(i))
for ts, pkt in pcap.pcap(name='./probes/probes-2013-03-30.pcap4'):
	try:
		rtap = dpkt.radiotap.Radiotap(pkt)
	except:
		pass
	wifi = rtap.data
	if wifi.type == 0 and wifi.subtype == 4:
		if nPkts != 0:
			intervaloProbes = lista[nPkts]-lista[nPkts-1]
			# print("intervaloProbes: %f"%(intervaloProbes/runtime))
			src = binascii.hexlify(wifi.mgmt.src)
			if len(wifi.ies) != 0:
				ssid = wifi.ies[0].info
				if ssid.strip() == '':
					ssid = 'BROADCAST'
			else:
				ssid = None

			#if ssid != "BROADCAST" and ssid not in lista1:
				#lista1.append(ssid)

			# print("intervalo do loop: %f"%(timeit.default_timer()-startProbe))
			try:
				time.sleep((intervaloProbes - (timeit.default_timer()-startProbe))/runtime)
			except:
				pass
				
			startProbe = timeit.default_timer()
		else:
			src = binascii.hexlify(wifi.mgmt.src)
			if len(wifi.ies) != 0:
				ssid = wifi.ies[0].info
				if ssid.strip() == '':
					ssid = 'BROADCAST'
			else:
				ssid = None

			#if ssid != "BROADCAST" and ssid not in lista1:
				#lista1.append(ssid)

			start,startProbe = timeit.default_timer(), timeit.default_timer()
		nPkts += 1
		firstMac = src[:6].upper()
		try:
			vendorName= dados[firstMac]
		except:
			vendorName= 'Unknown'
		print('%f,%s,%s,%s'%(ts,src,ssid, vendorName))
		producer.send('rsipsd_project', '%f,%s,%s,%s'%(ts,src,ssid,vendorName))
	if (nPkts == maxPkts):
		break

end = timeit.default_timer()
print("Time: %f, tempoEsperado: %fs"%((end - start), tempoEsperado/runtime))

#print(len(lista1))
#print(lista1)