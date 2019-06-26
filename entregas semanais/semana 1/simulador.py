import pcap, dpkt, binascii, timeit, time, sys, pika

if len(sys.argv) != 2:#6:
	print >> sys.stderr, "Usage: %s [simulador.py Runtime] [RabbitMQ host] [RabbitMQ user] [RabbitMQ password] [RabbitMQ vhost]..." % (sys.argv[0])
	sys.exit(1)
else:
	try:
		if float(sys.argv[1]) < 1:
			sys.exit(1)
		runtime = float(sys.argv[1])
	except:
		print "[runtime] Must be of type number and >= 1"
		sys.exit(1)

host = "192.168.0.103" #sys.argv[2]
user = "EHL" #sys.argv[3]
passwd = "123" #sys.argv[4]
vhost = "RSIPSD" #sys.argv[5]

credentials = pika.PlainCredentials(user, passwd)
connection = pika.BlockingConnection(pika.ConnectionParameters(host, 5672, vhost, credentials))
channel = connection.channel()
channel.exchange_declare(exchange='rsipsd_project',
                         exchange_type='topic')

maxPkts = 100
nPkts=0
lista=[]
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
		if nPkts !=0:
			intervaloProbes = lista[nPkts]-lista[nPkts-1]
			# print("intervaloProbes: %f"%(intervaloProbes/runtime))
			src = binascii.hexlify(wifi.mgmt.src)
			if len(wifi.ies) != 0:
				ssid = wifi.ies[0].info
			else:
				ssid = None
			# print("intervalo do loop: %f"%(timeit.default_timer()-startProbe))
			if (timeit.default_timer()-startProbe) < intervaloProbes:
				time.sleep((intervaloProbes - (timeit.default_timer()-startProbe))/runtime)
			startProbe = timeit.default_timer()
		else:
			src = binascii.hexlify(wifi.mgmt.src)
			if len(wifi.ies) != 0:
				ssid = wifi.ies[0].info
			else:
				ssid = None
			start,startProbe = timeit.default_timer(), timeit.default_timer()
		channel.basic_publish(exchange='rsipsd_project', routing_key='probe', body='%f,%s,%s'%(ts,src,ssid))
		nPkts += 1
	# if (nPkts == maxPkts):
	# 	break

connection.close()

end = timeit.default_timer()
print("Time: %f, tempoEsperado: %fs"%((end - start), tempoEsperado/runtime))