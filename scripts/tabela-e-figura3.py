import pcap, dpkt, binascii
# maxPkts = 1
nPkts=0

number_of_probes = 0
listaMAC = {}
PNLs = 0
directed_probes=0
broadcast_probes=0
listaSSID=[]

for i in range(7):
	print('probes-2013-03-30.pcap'+str(i))
	for ts, pkt in pcap.pcap(name='./probes/probes-2013-03-30.pcap'+str(i)):
		try:
			rtap = dpkt.radiotap.Radiotap(pkt)
		except:
			pass
		wifi = rtap.data
		if wifi.type == 0 and wifi.subtype == 4:
			src = binascii.hexlify(wifi.mgmt.src)
			if len(wifi.ies) != 0:
				ssid = wifi.ies[0].info
				if (ssid not in listaSSID):
					if ssid.strip() != '':
						listaSSID.append(ssid)
			else:
				ssid = None
				print(ts)

			nPkts += 1
			if src not in listaMAC:
				listaMAC[src] = []

			if ssid != None:
				if ssid.strip() == '':
					broadcast_probes += 1
				else:
					# print(ssid)
					directed_probes += 1
					if len(listaMAC[src]) == 0:
						PNLs +=1
					if ssid not in listaMAC[src]:
						listaMAC[src].append(ssid)
				number_of_probes += 1

	# if (nPkts == maxPkts):
	# 	break

print("Probes: "+str(number_of_probes))
# print(listaMAC)
print("Devices: "+str(len(listaMAC)))
print("PNLs: "+str(PNLs))
print("Directed Probes: "+str(directed_probes))
print("Broadcasted Probes: "+str(broadcast_probes))
print("SSID: "+str(len(listaSSID)))
arquivo = open('tabela-estatistica.csv','w')
arquivo.write('Dataset,Devices,PNLs,Total_Probes,Directed_Probes,Broadcast_Probes,SSIDs\n')
arquivo.write('The Mall(M),'+str(len(listaMAC))+','+str(PNLs)+','+str(number_of_probes)+','+str(directed_probes)+','+str(broadcast_probes)+','+str(len(listaSSID)))
arquivo.close()

################################(Figure 3.b)PNL length#################################################

PNLLength = list(map(lambda x: str(len(listaMAC[x])) ,listaMAC))
PNLDispositivos = {}

#Conteudo do "for"
def atualizaPNLDispositivos (pnlLength):
	if pnlLength not in PNLDispositivos:
		PNLDispositivos[pnlLength] = 0 
	PNLDispositivos[pnlLength] += 1

map(atualizaPNLDispositivos, PNLLength)

# for i in PNLLength:
# 	if i not in PNLDispositivos:
# 		PNLDispositivos[i] = 0
# 	PNLDispositivos[i] += 1

del PNLDispositivos['0']

dictSorted = sorted(PNLDispositivos.items(), key = lambda e: int(e[0]), reverse = False)
dictFormated = list(map(lambda x: '%s,%d\n'%(x[0], x[1]), dictSorted))
a = open('PNLLength.csv','w')
a.write('Length_of_the_PNL,Number_of_Devices\n')
a.writelines(dictFormated)
a.close()

##########################(Figure 3.a)Number of SSID x Number of Devices#################################

ocorrenciaSSIDs = {}

def atualizaOcorrenciaSSIDs(ssid):
	if ssid not in ocorrenciaSSIDs:
		ocorrenciaSSIDs[ssid] = 0
	ocorrenciaSSIDs[ssid] += 1

# def conteudoListaSSIDs(listaSSIDs):
# 	map(atualizaOcorrenciaSSIDs, listaSSIDs)

map(lambda listaSSIDs: map(atualizaOcorrenciaSSIDs, listaSSIDs[1]), listaMAC.items())
# for i in listaMAC.items():
#     for j in i[1]:
#         if j not in ocorrenciaSSIDs:
#             ocorrenciaSSIDs[j] = 0
#         ocorrenciaSSIDs[j] += 1
# print(ocorrenciaSSIDs)

dispositivosSSID={}

def atualizaDispositivosSSID(dispositivos):
	if str(dispositivos[1]) not in dispositivosSSID:
		dispositivosSSID[str(dispositivos[1])] = 0
	dispositivosSSID[str(dispositivos[1])] +=1

map(atualizaDispositivosSSID,ocorrenciaSSIDs.items())

# for i in ocorrenciaSSIDs.items():
# 	if str(i[1]) not in dispositivosSSID:
# 		dispositivosSSID[str(i[1])] = 0
# 	dispositivosSSID[str(i[1])] += 1


dictSorted = sorted(dispositivosSSID.items(), key = lambda e: int(e[0]), reverse = False)
dictFormated = list(map(lambda x: '%s,%d\n'%(x[0], x[1]), dictSorted))
a = open('SSIDs.csv','w')
a.write('Number_of_Devices,Number_of_SSIDs\n')
a.writelines(dictFormated)
a.close()