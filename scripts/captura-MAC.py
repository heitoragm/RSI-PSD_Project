import pcap, dpkt, binascii

# maxPkts = 1
nPkts=0

arquivo = open('vendorMacs.prop', 'r')
dados=arquivo.read()
arquivo.close()
dados = dados.split('\n')[:-1]
dados = dict(map(lambda x: (x.split('=')[0], x.split('=')[1]), dados))
devices = []
vendors = {}

for i in range(7):
    print('probes-2013-03-30.pcap'+str(i))
    for ts, pkt in pcap.pcap(name='./probes/probes-2013-03-30.pcap'+str(i)):
        # if nPkts%100 == 0:
        #     print(nPkts) 
        try:
            rtap = dpkt.radiotap.Radiotap(pkt)
        except:
            pass
        wifi = rtap.data
        if wifi.type == 0 and wifi.subtype == 4:
            src = binascii.hexlify(wifi.mgmt.src)
            if len(wifi.ies) != 0:
                ssid = wifi.ies[0].info
            else:
                print(ts)

            nPkts += 1

            if src not in devices:
                devices.append(src)
                firstMAC = src[:6].upper()
                if firstMAC not in dados:
                    if 'Unknown' not in vendors:
                        vendors['Unknown'] = 0
                    vendors['Unknown'] += 1
                else:
                    if dados[firstMAC] not in vendors:
                        vendors[(dados[firstMAC])] = 0
                    vendors[dados[firstMAC]] += 1

        # if (nPkts == maxPkts):
        #     break

# print(vendors)
listFormated = list(map(lambda x: '%s,%d\n'%(x, vendors[x]),vendors))
arquivo = open('capturas-mac.csv', 'w')
arquivo.write('Vendors,Number_of_Devices\n')
arquivo.writelines(listFormated)
arquivo.close()
