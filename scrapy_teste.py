import scrapy
import ipdb
import pandas as pd
from datetime import date
import os
import time
from multiprocessing import Pool,cpu_count

class AgenciaBrasil(scrapy.Spider):
    name = 'agenciabrasil'
    start_urls = ['https://agenciabrasil.ebc.com.br/ultimas?page=',
                  'http://agenciaal.alesc.sc.gov.br/index.php/noticias',
                  'https://www.blumenau.sc.gov.br/listagem/noticias',
                  'http://www.sc.gov.br/noticias/',
                  'http://www.camarablu.sc.gov.br/comunicacao/releases/?future=false',
                  'https://www.tjsc.jus.br/web/imprensa/noticias',
                  'https://portal.brusque.sc.gov.br/noticias/',
                  'http://www.navegantes.sc.gov.br/noticias',
                  'https://itajai.sc.gov.br/noticias',
                  'https://www.bc.sc.gov.br/imprensa.cfm',
                  'https://www.mpsc.mp.br/noticias/noticias-mpsc']
    try:
        f = open("titulos.csv","w+")
    except:
        os.system("taskkill /im EXCEL.EXE /f")
        time.sleep(2)
        f = open("titulos.csv","w+")
    i =0 #resolução problema (UnboundLocalError: local variable 'i' referenced before assignment) no metodo parseBnu
        
    def start_requests(self):
        agencia_brasil = self.start_urls[0]
        for i in range(0,3):
            yield scrapy.Request(agencia_brasil+str(i),self.parseAgenciaBrasil)
        
        alesc = self.start_urls[1]
        yield scrapy.Request(alesc,self.parseAlesc)
        
        prefeitura_bnu = self.start_urls[2]
        yield scrapy.Request(prefeitura_bnu,self.parseBnu)
        
        gov_sc = self.start_urls[3]
        yield scrapy.Request(gov_sc,self.parseGov)
        
        camara_vereadores = self.start_urls[4]
        yield scrapy.Request(camara_vereadores,self.parseVereadores)
        
        tjsc = self.start_urls[5]
        yield scrapy.Request(tjsc,self.parseTJSC)
        
        prefeitura_brusque = self.start_urls[6]
        yield scrapy.Request(prefeitura_brusque,self.parseBrusque)
        
        prefeitura_navegantes = self.start_urls[7]
        yield scrapy.Request(prefeitura_navegantes,self.parseNavegantes)
        
        prefeitura_itajai = self.start_urls[8]
        yield scrapy.Request(prefeitura_itajai,self.parseItajai)
        
        #prefeitura_bc = self.start_urls[9]
        #yield scrapy.Request(prefeitura_bc,self.parseBC)
        
        mpsc = self.start_urls[10]
        yield scrapy.Request(mpsc,self.parseMPSC)
    
    def parseAgenciaBrasil(self,response):
        #ipdb.set_trace()
        #pass
        links = response.css('.post-item-desc.py-0>a::attr(href)').getall()
        for link in links:
            if link.count('/')<2:
                links.remove(link)
        titulos =  response.xpath('//h4/text()').getall()
        for i in range(len(titulos)):
            titulos[i] += '             link: https://agenciabrasil.ebc.com.br'+links[i]
        titulos = self.prepend(titulos,'Agencia Brasil: ')
        df = pd.DataFrame(titulos, columns=["colummn"])
        try:
            df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
        except:
            df.to_csv('titulos.csv', mode='a',index=False,encoding='utf-8')
        
    def parseAlesc(self,response):
        #ipdb.set_trace()
        #pass
        links = response.css('.item_gabinete> h1>a::attr(href)').getall()#links das noticias
        titulos = response.css('.item_gabinete> h1>a::text').getall()
        dias = response.css('.item_gabinete>.data_list_gabinete.doze.light_grey::text').getall()
        for n in range(len(dias)):
            dia = dias[n]
            dias[n] = dia[3:8]
        titulos_entrou = self.filtraDia(titulos,links,dias,0,'Alesc: ',True)
        df = pd.DataFrame(titulos_entrou, columns=["colummn"])
        df.to_csv('titulos.csv', mode='a',index=False,encoding='latin1')
        
    def parseBnu(self,response):
        #ipdb.set_trace()
        #pass
        titulos = response.xpath('//ul[@id="scroler"]//li//span//h2/text()').getall()
        links = response.xpath('//ul[@id="scroler"]//li//a/@href').getall()
        horarios = response.xpath('//ul[@id="scroler"]//li//a//strong/text()').getall()
        for h in range(len(horarios)):
            hora1 = horarios[h]
            hora2 = horarios[h+1]
            if int(hora1[0:2]) < int(hora2[0:2]):
                break;
        horarios = horarios[0:h+1]
        for self.i in range(h): #usar self para corrigir erro
            titulos[self.i] += '             link: '+links[self.i]
        titulos = titulos[0:self.i+1]
        titulos = self.prepend(titulos,'Prefeitura BNU: ')
        df = pd.DataFrame(titulos, columns=["colummn"])
        df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
        
    def parseGov(self,response):
        #ipdb.set_trace()
        #pass
        titulos_principais = response.css('h3.allmode-title>a::text').getall()
        links_principais = response.css('h3.allmode-title>a::attr(href)').getall() 
        titulos_baixo = response.css('h4.allmode-title>a::text').getall()
        links_baixo = response.css('h4.allmode-title>a::attr(href)').getall()
        dias = response.css('div.allmode-date::text').getall() + response.css('span.allmode-date::text').getall()
        titulos = titulos_principais+titulos_baixo
        links = links_principais + links_baixo
        links = ['http://www.sc.gov.br'+link for link in links]
        dias = [dia[:-5] for dia in dias]
        #print(dias)
        titulos = self.filtraDia(titulos,links,dias,0,'Governo SC: ',True)
        df = pd.DataFrame(titulos, columns=["colummn"])
        df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
        
    def parseVereadores(self,response):
        #ipdb.set_trace()
        #pass
        titulos = response.css('ul.noticias-lista>li>h2>a::text').getall()
        links = response.css('ul.noticias-lista>li>h2>a::attr(href)').getall()
        dias = response.css('small.date_post::text').getall()
        dias = [dia[:-5] for dia in dias]
        #print(dias)
        titulos = self.filtraDia(titulos,links,dias,0,'Camara de vereadores de Blumenau: ',True)
        df = pd.DataFrame(titulos, columns=["colummn"])
        df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
        
    def parseTJSC(self,response):
       # ipdb.set_trace()
        #pass
        titulos = response.css('.row>.col-xs-12.fw3-asset-publisher-list-asset-entry>.fw3-asset-publisher-list-asset-entry-title>a::text').getall()
        links = response.css('.row>.col-xs-12.fw3-asset-publisher-list-asset-entry>.fw3-asset-publisher-list-asset-entry-title>a::attr(href)').getall()        
        dias = response.css('.row>.col-xs-12.fw3-asset-publisher-list-asset-entry>.fw3-asset-publisher-list-asset-entry-date-time>span.fw3-asset-publisher-list-asset-entry-date-time-date::text').getall()
        #print(dias)
        dias = [dia[1:-6] for dia in dias]
        #print(dias)
        titulos = self.filtraDia(titulos,links,dias,0,'TJSC: ',True)
        df = pd.DataFrame(titulos, columns=["colummn"])
        df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
        
    def parseBrusque(self,response):
        #ipdb.set_trace()
        #pass
        titulos = response.css('a>h4::text').getall()
        links = response.css('.content-blog.my-2>a::attr(href)').getall()
        dias  = response.css('.content-blog.my-2>small.text-muted::text').getall()
        dia_atual = date.today().day
        dias = [dia for dia in dias if str(dia_atual) in dia]
        dias = [dia[6:-15].replace('de','') for dia in dias]
        dias = [dia.replace(' ','',1) for dia in dias]
        dia = dias[0]
        if int(dia[0])>2:
            dias = self.prepend(dias,'0')
        titulos_entrou = []
        for i in range(len(dias)):
            titulos_entrou.append(titulos[i])
        titulos = self.filtraDia(titulos_entrou,links,dias,0,'Prefeitura de Brusque: ',True)
        df = pd.DataFrame(titulos, columns=["colummn"])
        try:
            df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
            #return None
        except:
            df.to_csv('titulos.csv', mode='a',index=False,encoding='utf-8')
            #return None
            
        
    def parseNavegantes(self,response):
        #ipdb.set_trace()
        #pass
        titulos = response.css('.dpag_noticia_titulo::text').getall()
        dias = response.css('.dpag_noticia_data::text').getall()
        links = response.css('.dpag_noticia_dados>a::attr(href)').getall()
        dias = [dia[0:5] for dia in dias]
        links = ['http://www.navegantes.sc.gov.br'+link for link in links]
        titulos = self.filtraDia(titulos,links,dias,0,'Prefeitura de Navegantes: ',True)
        df = pd.DataFrame(titulos, columns=["colummn"])
        df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
        
    def parseItajai(self,response):
        #ipdb.set_trace()
        #pass
        titulos = response.css('.dpag_noticia_titulo::text').getall()
        dias = response.css('.dpag_noticia_data::text').getall()
        links = response.css('.dpag_noticia_dados>a::attr(href)').getall()
        dias = [dia[0:5] for dia in dias]
        links = ['https://itajai.sc.gov.br'+link for link in links]
        titulos = self.filtraDia(titulos,links,dias,0,'Prefeitura de Itajai: ',True)
        df = pd.DataFrame(titulos, columns=["colummn"])
        df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
        
    '''def parseBC(self,response):
        #ipdb.set_trace()
        #pass
        titulos = response.css('a.caller.black::text').getall()
        links = response.css('a.caller.black::attr(href)').getall()
        dias_aux = response.css('.list-unstyled.lista-links.mt-1.mb-2>li::text').getall()
        for lista in response.css('ul.list-unstyled.lista-links'):
            for info in lista.css('li'):
                print(info.css('a::text').extract()[-1])'''
                
    '''def close(self,reason):
        self.f.close()
        time.sleep(10)
        os.startfile("titulos.csv")'''
        
    def parseMPSC(self,response):
        #ipdb.set_trace()
        #pass
        titulos = response.css('h4.post-title.font-bold::text').getall()
        titulos = [titulo for titulo in titulos if titulo != '\r        ' and 
                   titulo != '\r            ' and 
                   titulo!='\r\n            ' and
                   titulo != '\r\n        ']
        titulos = [titulo.replace('\r            ','') for titulo in titulos]
        titulos = [titulo.replace('\r\n            ','') for titulo in titulos]
        titulos = titulos[:len(titulos)-1]
        links = response.css('a::attr(href)').getall()
        links = [link for link in links if 'noticias' in link]
        links = links[2:len(links)-3]
        for i in range(0,7):
            titulos[i] = titulos[i]+"           link: https://www.mpsc.mp.br"+links[i]
        titulos = titulos[:i]
        df = pd.DataFrame(titulos, columns=["colummn"])
        df.to_csv('titulos.csv', mode='a',index=False,encoding='iso-8859-1')
        
        
    def prepend(self,titulos,nome):
        nome += '{0}'
        titulos = [nome.format(titulo) for titulo in titulos]
        return titulos
        
    def filtraDia(self,titulos,links,dias,numero,portal,mes):
        titulos_entrou = []
        data_atual = date.today()
        if(len(dias) == len(titulos)):
            if(not mes):
                for i in range(len(titulos)):
                    dia  = dias[i]
                    if int(dia[0:2])==int(data_atual.day):    
                        titulos[i] += '             link: '+links[i]
                        titulos_entrou.append(titulos[i])
                titulos_entrou = self.prepend(titulos_entrou,portal)
                return titulos_entrou
            else:
                try:
                    '''if portal=='Alesc: ':
                        ipdb.set_trace()'''
                    for i in range(len(titulos)):
                        dia  = dias[i]
                        if (int(dia[0:2])==int(data_atual.day)) and (int(dia[3:])==int(data_atual.month)):    
                            titulos[i] += '             link: '+links[i]
                            titulos_entrou.append(titulos[i])
                    titulos_entrou = self.prepend(titulos_entrou,portal)
                    return titulos_entrou
                except:
                    meses = ['aneiro','evereiro','arço','bril','aio','unho','ulho','gosto','etembro','utubro','ovembro','ezembro']
                    mes_atual = meses[int(data_atual.month)-1]
                    for i in range(len(titulos)):
                        dia = dias[i]
                        #ipdb.set_trace()
                        if int(dia[0:2])==int(data_atual.day) and dia[4:]==mes_atual: 
                            #print("entrou           ")
                            titulos[i] += '             link: '+links[i]
                            titulos_entrou.append(titulos[i])
                    titulos_entrou = self.prepend(titulos_entrou,portal)
                    return titulos_entrou
        else:
            print("Numero de dias diferente do de titulos")
            print("Numero de titulos: "+str(len(titulos)))
            print("Numero dias: "+str(len(dias)))