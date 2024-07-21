import concurrent.futures
import datetime
import os
import re
import requests
import sqlite3
import time


start = time.time()
lastRequest = time.time()


class Stazione:
    lastId = 0

    def __init__(self, nome, rete = None, lat = None, lon = None,
                 quota = -1, quadrante = {'lat': 0, 'lon': 0}):
        self.nome = nome
        self.rete = rete
        self.lat = float(lat) if lat else lat
        self.lon = float(lon) if lon else lon
        self.quota = quota
        self.quadrante = {'lat': float(quadrante['lat']), 'lon': float(quadrante['lon'])}
        self.dati = {'secolo': [], 'tutti': []}
        self.idStazione = None
        self.idRete = None
        
    def __eq__(self, o):
        if type(o) != Stazione: return False
        if (self.nome.lower() == o.nome.lower() and
            self.rete.lower() == o.rete.lower()): return True
        return False

    def __str__(self):
        out = """nome: %s\nrete: %s\nlat: %s\nlon: %s\nquota: %s""" % (self.nome, self.rete, self.lat, self.lon, self.quota)
        return out
    
    def inQuadrante(self, stazione):
        lonOk = ((stazione.lon <= (self.quadrante['lon'] + 0.2) and
                 (self.quadrante['lon'] - 0.5) <= stazione.lon))
    
        latOk = ((stazione.lat >= (self.quadrante['lat'] -0.2) and
                 (self.quadrante['lat'] + 0.5) >= stazione.lat))
        return lonOk and latOk
        
    
    def cercaOpenStreetMap(self):
        def getUrls(par: str):
            
            def buildUrl(s):
                if type(s) == str: return 'https://nominatim.openstreetmap.org/search?q=' + s + '&format=json&polygon=1&addressdetails=1'
                return 'https://nominatim.openstreetmap.org/search?q=' + '+'.join(s) + '&format=json&polygon=1&addressdetails=1'
            
            # Rimuovo caratteri non alfabetici
            par = re.sub('[^a-zA-Z]+', '', par)
            par = par.split(' ')
            urls = [buildUrl(par)]
            if type(par) == list: urls += [buildUrl(_) for _ in par]
            return urls
        
        def trovaCorrispondenza(rList: list):
            def inQuadrante(stazione):
                lonOk = (float(stazione['lon']) <= (self.quadrante['lon'] + 0.2) and
                         (self.quadrante['lon'] - 0.5) <= float(stazione['lon']))
            
                latOk = (float(stazione['lat']) >= (self.quadrante['lat'] -0.2) and
                         (self.quadrante['lat'] + 0.5) >= float(stazione['lat']))
                return lonOk and latOk
                
            for stazione in rList:
                if stazione['address']['country'] == "Italia" and inQuadrante(stazione):
                    return ({'lat': stazione['lat'], 'lon': stazione['lon']})
            return None
        
        for url in getUrls(self.nome):
            h = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3",
                "Connection": "keep-alive",
                "DNT": "1",
                "Host": "nominatim.openstreetmap.org",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Sec-GPC": "1",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"}
            
            
            global lastRequest
            #if time.time() - lastRequest < 2: 
            #    print("I'm seleeping...")
            #    time.sleep(5)
            r = requests.get(url, headers=h)
            lastRequest = time.time()
            
            if r.status_code < 300:
                coord = trovaCorrispondenza(r.json())
                if coord: 
                    self.lat = coord['lat']
                    self.lon = coord['lon']
                    self.quota = -1
                    return True
            else: 
                print("Errore nella richiesta per la stazione %s\n%s\nErrore: %d\n\n" % (self.nome, url, r.status_code))
        return False
        
    def levDist(self, stazione):
        a = self.nome
        b = stazione.nome
        
        d = [[0 for _ in range(len(b) + 1)] for _ in range(len(a) + 1)]
        d[0] = list(range(len(b) + 1))
        for r in range(len(a) + 1): d[r][0] = r
        
        for j in range(1, len(b) + 1):
            for i in range(1, len(a) + 1):
                if a[i-1] == b[j-1]: 
                    subCost = 0
                else:
                    subCost = 1
                
                d[i][j] = min(d[i-1][j] + 1, d[i][j-1] + 1, d[i-1][j-1] + subCost)
                
        return d[-1][-1]    
        
    def cercaCorrispondenza(self, stazioni: list):
        punteggi = [(self.levDist(stazione), stazione) for stazione in stazioni][:10]
        for _ in punteggi:
            if self.inQuadrante(_[1]): 
                self.lat = _[1].lat
                self.lon = _[1].lon
                self.quota = _[1].quota
                return True
        return False
    
    def validaStazione(self, listaStazioni):
        # Cerca le coordinate della stazione
        if self in listaStazioni: 
            for stazione in listaStazioni:
                if self == stazione: 
                    print(self)
                    return
                    self.lat = stazione.lat
                    self.lon = stazione.lon
                    self.quota = stazione.quota
                    return True
                break
        if self.cercaCorrispondenza(listaStazioni):
            return True
        if self.cercaOpenStreetMap():
            return True
        return False


class dbManager:
    def __init__(self, nome: str):
        self.nome = nome
        self.con = sqlite3.connect(nome, check_same_thread=False)
        self.cur = self.con.cursor()
    
    def clearTable(self, table):
        self.cur.execute("delete from " + table)
        self.con.commit()
        
    def clearDatabase(self):
        self.con.close()
        self.con = sqlite3.connect(self.nome)
        self.con.execute("delete from Reti")
        self.con.execute("delete from Stazioni")
        self.con.execute("delete from PrecipitazioniSecolo")
        self.con.execute("delete from Precipitazioni")
        self.con.commit()
        self.cur = self.con.cursor()
    
    def insertPrecipitazioniTutte(self, stazione: Stazione):
        data = [(stazione.idRete, stazione.idStazione) + _ for _ in stazione.dati['tutti']]
        self.cur.executemany("insert into Precipitazioni values (?,?,?,?)", data)
        self.con.commit()
    
    def insertPrecipitazioniSecolo(self, stazione):
        data = [(stazione.idRete, stazione.idStazione) + _ for _ in stazione.dati['secolo']]
        self.cur.executemany("insert into PrecipitazioniSecolo values (?,?,?,?)", data)
        self.con.commit()
    
    def insertRete(self, idRete, rete):
        self.cur.executemany("insert into Reti values(?,?)", [(idRete, rete),])
        self.con.commit()
        
    def insertStazione(self, stazione):
        self.cur.executemany("insert into Stazioni values(?,?,?,?,?,?)", [(int(stazione.idStazione), int(stazione.idRete), stazione.nome, float(stazione.lat), float(stazione.lon), float(stazione.quota)),])
        self.con.commit()
        
    
class GestoreStazioni:
    def __init__(self, threads):
        self.threads = threads
        self.pathStazioniTrovate = "stazioniScaricate"
        self.pathStazioniInLista = "listaStazioni"
        self.cartellaCSV = './tuttiCSV/'
        self.db = dbManager('precipitazioni.db')
        self.allCSV = os.listdir(self.cartellaCSV)
        self.stazioniElaborate = []
        self.stazioniScartate = []
        self.idReti = {}
        self.idStazioni = {}
        self.tutteLeDate = [datetime.datetime.fromtimestamp(t).strftime("%d-%m-%Y") for t in range(-1514768400, 1672527600, 60*60*24)]
        # Carico le stazioni in lista
        self.getStazioniInLista()
    
    
    def caricaCSV(self, path: str):
        with open(path, 'r', encoding="utf-8") as f:
            content = f.read()
            content = content.replace('"', '')
            content = content.split("\n")[:-1]
            content = [riga.split(",") for riga in content][:-1]
        return content

    def getStazioniInLista(self):
        self.stazioniInLista = [
            Stazione(
                rete = riga[0],
                nome = riga[1],
                lon = riga[3],
                lat = riga[4],
                quota = riga[5]
                )
            for riga in self.caricaCSV(self.pathStazioniInLista)[1:]]
        return 
    
    def correggiData(self, d: str):
        mesi = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        newD = d.split('-')
        newD = '-'.join([newD[1], "%02d" % (mesi.index(newD[0]) + 1), newD[2]])
        return newD
    
    def aggiungiDateMancanti(self, listaTuple: list):
        dictDate = {_[0]:_[1] for _ in listaTuple}
        return [(_, dictDate[_]) if _ in dictDate else (_, -1) for _ in self.tutteLeDate]
   
    def getStack(self, content, quadrante):
        stackStazioni = []
        singoleStazioni = set([(_[0], _[1]) for _ in content])
        stackStazioni = []
        for stazione in singoleStazioni:
            tmpStazione = Stazione(nome=stazione[1], rete=stazione[0], quadrante=quadrante)
            tmpStazione.dati['tutti'] = [(self.correggiData(x[2]),x[3]) for x in content if (x[0] == tmpStazione.rete and x[1] == tmpStazione.nome)]
            tmpStazione.dati['secolo'] = self.aggiungiDateMancanti(tmpStazione.dati['tutti'])
            stackStazioni += [tmpStazione]
                        
        return stackStazioni
    
    def assegnaIdRete(self, stazione):
        if stazione.rete in self.idReti.keys():
            stazione.idRete = self.idReti[stazione.rete]
        else:
            if self.idReti.values():
                newId = max(self.idReti.values()) + 1
            else:
                newId = 1
            stazione.idRete = newId
            self.idReti[stazione.rete] = newId
            self.db.insertRete(newId, stazione.rete) 
        return
    
    def assegnaIdStazione(self, stazione: Stazione):
        # Se la funzione viene chiamata vuol dire che la stazione non è stata elaborata
        if self.idStazioni.values():
            newId = max(self.idStazioni.values()) + 1
        else:
            newId = 1
        stazione.idStazione = newId
        self.idStazioni[stazione.nome] = newId
        # self.db.insertStazione(stazione)
        return 
        
    
    def elaboraFile(self, path: str):
        def elaboraStazione(self, stazione):
            if stazione in self.stazioniElaborate: return
            self.assegnaIdRete(stazione)  
            self.stazioniElaborate.append(stazione)
            if(stazione.validaStazione(self.stazioniInLista)):
                # Coordinate trovate
                self.assegnaIdStazione(stazione)
                self.db.insertStazione(stazione)
                self.db.insertPrecipitazioniTutte(stazione)
                self.db.insertPrecipitazioniSecolo(stazione)
                # Rimuovo i dati alle stazioni inserite per liberare spazio
                stazione.dati = {'secolo': [], 'tutti': []}
                print("Aggiungo " + stazione.nome)
            else:
                # Coordinate non trovate
                self.stazioniScartate += [stazione]
                print("Scarto " + stazione.nome)
            
        def file2coord(s: str):
            lat = int(s[0:2]) + (int(s[2:4]) / 60)
            lon = int(s[5:7]) + (int(s[7:9]) / 60)
            return {'lat': lat, 'lon': lon}
        
        content = self.caricaCSV(self.cartellaCSV + path)[1:]
        stack = self.getStack(content, file2coord(path))
        
        for stazione in stack:
            elaboraStazione(self, stazione)
        """
        for stazione in stack:
            if stazione in self.stazioniElaborate: continue
            self.assegnaIdRete(stazione)  
            self.stazioniElaborate += [stazione]
            if(stazione.validaStazione(self.stazioniInLista)):
                # Coordinate trovate
                self.assegnaIdStazione(stazione)
                self.db.insertStazione(stazione)
                self.db.insertPrecipitazioniTutte(stazione)
                self.db.insertPrecipitazioniSecolo(stazione)
            else:
                # Coordinate non trovate
                self.stazioniScartate += [stazione]
              
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
            th = [executor.submit(elaboraStazione, self, stazione) for stazione in stack]
            for future in concurrent.futures.as_completed(th):
                print("Terminato " + str(future.result))
        
        return stack
    
    def start(self):
        counter = 1
        maxCounter = len(self.allCSV)
        for csv in self.allCSV:
            print("Elaboro {}... {} di {} - {}%".format(csv, counter, maxCounter, int((counter/maxCounter)*100)))
            gestoreStazioni.elaboraFile(csv)
            counter += 1

    
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    gestoreStazioni = GestoreStazioni(threads=2)
    gestoreStazioni.db.clearDatabase()
    gestoreStazioni.start()
    
    print("Tempo passato: " + str(time.time() - start))