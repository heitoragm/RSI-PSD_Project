import pcap, dpkt, binascii
import itertools, math

# maxPkts = 1
nPkts=0

arquivo = open('vendorMacs.prop', 'r')
dados=arquivo.read()
arquivo.close()
dados = dados.split('\n')[:-1]
dados = dict(map(lambda x: (x.split('=')[0], x.split('=')[1]), dados))
vendors = {}

number_of_probes = 0
listaMAC = {}
PNLs = {'total': 0}
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


			if ssid != None:
				nPkts += 1
				if src not in listaMAC:
					listaMAC[src] = []

					firstMAC = src[:6].upper()
					if firstMAC not in dados:
						if 'Unknown' not in vendors:
							vendors['Unknown'] = 0
							PNLs['Unknown'] = 0
						vendors['Unknown'] += 1
					else:
						if dados[firstMAC] not in vendors:
							vendors[(dados[firstMAC])] = 0
							PNLs[dados[firstMAC]] = 0 
						vendors[dados[firstMAC]] += 1
				
				if ssid.strip() == '':
					broadcast_probes += 1
				else:
					# print(ssid)
					directed_probes += 1
					if len(listaMAC[src]) == 0:
						PNLs["total"]+=1

						firstMAC = src[:6].upper()
						if firstMAC not in dados:
							PNLs['Unknown'] += 1
						else:
							PNLs[dados[firstMAC]] += 1

					if ssid not in listaMAC[src]:
						listaMAC[src].append(ssid)
				number_of_probes += 1

	# if (nPkts == maxPkts):
	# 	break

SSIDsComMACs = {}

def atualizaSSIDsComMACs(mac,ssid):
	if ssid not in SSIDsComMACs:
		SSIDsComMACs[ssid] = []
	SSIDsComMACs[ssid].append(mac)

map(lambda listaSSIDs: map(lambda ssid: atualizaSSIDsComMACs(listaSSIDs[0],ssid), listaSSIDs[1]), listaMAC.items())

######################################(Table 2)########################################################

def percentageString(numerator, denominator):
	return ' (%1.1f%%)'%((float(numerator)/denominator)*100)

print("Probes: "+str(number_of_probes))
# print(listaMAC)
print("Devices: "+str(len(listaMAC)))
print("PNLs: "+str(PNLs['total']))
print("Directed Probes: "+str(directed_probes))
print("Broadcasted Probes: "+str(broadcast_probes))
print("SSID: "+str(len(listaSSID)))
# arquivo = open('tabela-estatistica.csv','w')
# arquivo.write('Dataset,Devices,PNLs (%),Total_Probes,Directed_Probes (%),Broadcast_Probes (%),SSIDs\n')
# arquivo.write('The Mall(M),'+str(len(listaMAC))+','+str(PNLs['total'])+percentageString(PNLs['total'],len(listaMAC))+','+str(number_of_probes)+','+str(directed_probes)+percentageString(directed_probes,number_of_probes)+','+str(broadcast_probes)+percentageString(broadcast_probes,number_of_probes)+','+str(len(listaSSID)))
# arquivo.close()

#################################(Figure 1) Vendors Distribution#######################################

# listFormated = list(map(lambda x: '%s,%d\n'%(x, vendors[x]),vendors))
# arquivo = open('capturas-mac.csv', 'w')
# arquivo.write('Vendors,Number_of_Devices\n')
# arquivo.writelines(listFormated)
# arquivo.close()

#################################(Figure 2) Vendors Exposing PNL#######################################

# def listaPNLVendors(tuple):
# 	devices = vendors[tuple[0]]
# 	return "%s,%d,%d,%1.0f%%\n"%(tuple[0], tuple[1], devices, (float(tuple[1])/devices)*100)

# # for i in PNLs:
# # 	if i == 'total':
# # 		continue
# print(PNLs)

# listFormated = list(map(lambda tuple: listaPNLVendors(tuple),filter(lambda tuple: tuple[0] != 'total',PNLs.items())))
# print(listFormated)
# print(len(listFormated), len(PNLs))
# arquivo = open('FornecedoresExpostos.csv', 'w')
# arquivo.write('Vendors,Exposeds,Number_of_Devices,Percent\n')
# arquivo.writelines(listFormated)
# arquivo.close()

################################(Figure 3.b)PNL length#################################################

# PNLLength = list(map(lambda x: str(len(listaMAC[x])) ,listaMAC))
# PNLDispositivos = {}

# #Conteudo do "for"
# def atualizaPNLDispositivos (pnlLength):
# 	if pnlLength not in PNLDispositivos:
# 		PNLDispositivos[pnlLength] = 0 
# 	PNLDispositivos[pnlLength] += 1

# map(atualizaPNLDispositivos, PNLLength)

# # for i in PNLLength:
# # 	if i not in PNLDispositivos:
# # 		PNLDispositivos[i] = 0
# # 	PNLDispositivos[i] += 1

# del PNLDispositivos['0']

# dictSorted = sorted(PNLDispositivos.items(), key = lambda e: int(e[0]), reverse = False)
# dictFormated = list(map(lambda x: '%s,%d\n'%(x[0], x[1]), dictSorted))
# arquivo = open('PNLLength.csv','w')
# arquivo.write('Length_of_the_PNL,Number_of_Devices\n')
# arquivo.writelines(dictFormated)
# arquivo.close()

##########################(Figure 3.a)Number of SSID x Number of Devices#################################

# ocorrenciaSSIDs = {}

# def atualizaOcorrenciaSSIDs(ssid):
# 	if ssid not in ocorrenciaSSIDs:
# 		ocorrenciaSSIDs[ssid] = 0
# 	ocorrenciaSSIDs[ssid] += 1

# # def conteudoListaSSIDs(listaSSIDs):
# # 	map(atualizaOcorrenciaSSIDs, listaSSIDs)

# map(lambda listaSSIDs: map(atualizaOcorrenciaSSIDs, listaSSIDs[1]), listaMAC.items())
# # for i in listaMAC.items():
# #     for j in i[1]:
# #         if j not in ocorrenciaSSIDs:
# #             ocorrenciaSSIDs[j] = 0
# #         ocorrenciaSSIDs[j] += 1
# # print(ocorrenciaSSIDs)

# dispositivosSSID={}

# def atualizaDispositivosSSID(dispositivos):
# 	if str(dispositivos[1]) not in dispositivosSSID:
# 		dispositivosSSID[str(dispositivos[1])] = 0
# 	dispositivosSSID[str(dispositivos[1])] +=1

# map(atualizaDispositivosSSID,ocorrenciaSSIDs.items())

# # for i in ocorrenciaSSIDs.items():
# # 	if str(i[1]) not in dispositivosSSID:
# # 		dispositivosSSID[str(i[1])] = 0
# # 	dispositivosSSID[str(i[1])] += 1


# dictSorted = sorted(dispositivosSSID.items(), key = lambda e: int(e[0]), reverse = False)
# dictFormated = list(map(lambda x: '%s,%d\n'%(x[0], x[1]), dictSorted))
# arquivo = open('SSIDs.csv','w')
# arquivo.write('Number_of_Devices,Number_of_SSIDs\n')
# arquivo.writelines(dictFormated)
# arquivo.close()

########################################(Table 3)##################################################

# medias = map(lambda tupla: float(len(tupla[1]))/len(listaMAC) ,SSIDsComMACs.items())

# v1 = str(len(listaMAC))
# v2 = str(len(listaSSID))
# listNumberLinks = list(map(lambda tuple: len(tuple[1]), filter(lambda tuple: len(tuple[1]) != 0,listaMAC.items())))
# print(len(listNumberLinks))
# E = sum(listNumberLinks)
# d1 = float(E)/len(listNumberLinks)
# d2 = sum(medias)
# print(E, d1, d2)
# arquivo = open('tabela-3.csv','w')
# arquivo.write('Dataset,|V1|,|V2|,|E|,d1,d2\n')
# arquivo.write('The Mall(M),'+str(v1)+','+str(v2)+','+str(E)+',%1.2f'%(d1)+',%1.2f'%(d2))
# arquivo.close()

#######################################(Calculo fADA)####################################################

#### Variavel futura ####

combinacoesMACs = []
threshold = 0.3
linksSociais = []

def atualizaCombinacoesMACs(L):
	combinacaoDupla = filter(lambda combinacao: len(combinacao) == 2 ,itertools.combinations(listaMAC, L))
	# print(combinacaoDupla)
	map(lambda subset: combinacoesMACs.append(subset), combinacaoDupla)

map(atualizaCombinacoesMACs,range(2, 3))
# for L in range(0, len(listaMAC)+1):
#     for subset in itertools.combinations(listaMAC, L):
#         print(subset) if len(subset) == 2 else None

# print(combinacoesMACs[:100])

def adamicAdam(combinacao):
	ssids1 = listaMAC[combinacao[0]]
	ssids2 = listaMAC[combinacao[1]]

	ssidsComum = filter(lambda ssid: ssid in ssids2,ssids1)
	popularidadeSSIDs = list(map(lambda ssid: len(SSIDsComMACs[ssid]), ssidsComum))
	
	somatorio = []
	def logBase2(valor):
		return float(1)/math.log(valor, 2)

	map(lambda tamanho: somatorio.append(logBase2(tamanho)) ,popularidadeSSIDs)

	if (sum(somatorio) >= threshold):
		# print(ssidsComum)
		# print(popularidadeSSIDs)
		# print(sum(somatorio))
		linksSociais.append([combinacao, ssidsComum, popularidadeSSIDs, sum(somatorio)])
		# print(combinacao)

map(adamicAdam ,combinacoesMACs)

listFormated = list(map(lambda x: '%s,%s,%s,%1.3f\n'%(str(x[0]),str(x[1]),str(x[2]),x[3]),linksSociais))
arquivo = open('links-sociais.csv', 'w')
arquivo.write('Combination,Common_SSIDs,SSIDs_Popularity,Adamic-Adar_Index,Total_Links_Sociais: %d'%(len(linksSociais))+'\n')
arquivo.writelines(listFormated)
arquivo.close()