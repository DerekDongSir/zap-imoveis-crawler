import urlListGenerator as urlgen
import json
import os
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup

class LargeScaleZapListMaker:
    def __init__(self, bairro='', zona='', cidade='', estado='', size='', chromiun_path='/usr/lib/chromium-browser/chromedriver', batch_size=300, path='url_list'):
        self.url_generator = urlgen.URL_list_generator(bairro, zona, cidade, estado, chromiun_path)
        self.url_list = self.url_generator.get_url_list()
        self.path = path
        self.batch_size = batch_size
        
    def chunks(self, l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]
    
    def organize_and_export_url_list(self):
        path = self.path
        a = 1
        for i in self.chunks(self.url_list, self.batch_size):
            with open(path+'/'+str(a)+'batch.json', 'w') as file:
                file.write(json.dumps(i))
            a += 1
        print("Stored", a-1, "lists at", path)


class LSperBatchMinifichaCrawler:
    def __init__(self, path="url_list", chromiun_path='/usr/lib/chromium-browser/chromedriver', path_to_store='minifichas_list'):
        self.chromiun_path = chromiun_path
        self.url_list_path = path
        self.url_lists_list = os.listdir(path)
        self.path_to_store = path_to_store
        
    def crawl_batch(self, batch_nmr):
        with open(self.url_list_path+"/"+str(batch_nmr)+'batch.json', 'r') as file:
            urls_to_crawl = json.load(file)
            mini_fichas = []
            i = 1
            for url in urls_to_crawl:
                chrome_options = webdriver.ChromeOptions()
                chrome_options.add_argument('--headless')
                driver = webdriver.Chrome(self.chromiun_path, chrome_options=chrome_options)
                driver.get(url)
                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
                alles = soup.find_all("article", {"class":"minificha"})
                mini_fichas.append(str(alles))
                driver.close()
                print('Crawled page', i, "from batch number", batch_nmr)
                i += 1
            return mini_fichas
    
    def store_batch_raw_info(self, batch_nmr):
        file_name = str(batch_nmr)+'batch-fichas.json'
        if file_name not in os.listdir(self.path_to_store):
            with open(self.path_to_store+"/"+file_name, 'w') as file:
                    file.write(json.dumps(self.crawl_batch(batch_nmr)))
                    print("Stored the html of batch", batch_nmr, "in path:", self.path_to_store+"/"+file_name)
        else:
            print("The batch", batch_nmr, "has alerady been crawled")
            

    def store_batch_list_raw_info(self, batch_nmr_tuple):
        for nmr in batch_nmr_tuple:
            self.store_batch_raw_info(nmr)
            
    def store_all_raw_html(self):
        urls_to_store = [i+1 for i in range(len(os.listdir(self.url_list_path)))]
        self.store_batch_list_raw_info(urls_to_store)
        
class LSJsonFichaParser:
    def __init__(self, jsonpath="minifichas_list", to_store_path='parsed_fichas'):
        self.to_store_path = to_store_path
        self.jsonpath = jsonpath
        self.minifichas_list = os.listdir(jsonpath)
    
    def import_and_soup_json(self, batch_nmr):
        file_name = str(batch_nmr)+'batch-fichas.json'
        with open(self.jsonpath+'/'+file_name, 'r') as f:
            fichas_list = json.load(f)
            fichas_soup_list = []
            for fichas_page in fichas_list:
                for ficha in fichas_list:
                    as_soup = BeautifulSoup(ficha, 'html.parser')
                    fichas_soup_list.append(as_soup)
            return fichas_soup_list

    def parse_and_store_page(self, batch_nmr):
        path = self.to_store_path
        file_name = str(batch_nmr)+'batch-parsed.json'
        if file_name not in os.listdir(path):
            to_parse_fichas_list = self.import_and_soup_json(batch_nmr)
            imoveis = []
            for item in to_parse_fichas_list:
                imovel = {}
                item_as_minificha = (item.find('article', {'class':'minificha'}))['data-clickstream']
                item_props = json.loads(item_as_minificha)
                imovel['estado'] = item_props['address'][1]
                imovel['cidade'] = item_props['address'][2]
                imovel['cep'] = item_props['address'][6]
                imovel['bairro'] = item_props['address'][3]
                imovel['rua'] = item_props['address'][4]
                imovel['valor_em_reais'] = item_props['salePrices'][0]
                
                try:
                    imovel['quartos'] = item_props['bedrooms'][0]
                except:
                    imovel['quartos'] = None
                try:
                    imovel['suites'] = item_props['suites'][0]
                except:
                    imovel['suites'] = None
                try:
                    imovel['vagas'] = item_props['parkingSpaces'][0]
                except: 
                    imovel['vagas'] = None
                try:
                    imovel['area'] = item_props['areas'][0]
                except:
                    imovel['area']=None
                
                imoveis.append(imovel)
            with open(path+'/'+file_name, 'w') as file:
                file.write(json.dumps(imoveis))
                print("Stored the PARSED html of batch", batch_nmr, "in path:", path+"/"+file_name)
            
        else:
            print("The batch", batch_nmr, "has alerady been PARSED")
        
    def parse_and_store_list(self, lists_to_store):
        for list_nmr in lists_to_store:
            self.parse_and_store_page(list_nmr)
    
    def parse_and_store_all(self, path_to_fichasHTML='minifichas_list'):
        lists_to_store = [i+1 for i in range(len(os.listdir(path_to_fichasHTML)))]
        self.parse_and_store_list(lists_to_store)

class RealEstateDataCSVgen:
    def __init__(self, parsed_json_path="parsed_fichas", csv_name='real_estate.csv'):
        self.csv_name = csv_name
        self.parsed_json_path = parsed_json_path
        
    def generate_df(self, batch_nmr):
        file_name = str(batch_nmr)+'batch-parsed.json'
        with open(self.parsed_json_path+'/'+file_name, 'r') as f:
            real_estate_dicts_list = json.load(f)
            return pd.DataFrame(real_estate_dicts_list)
        
    def create_csv(self):
        csv_name = self.csv_name
        exported_df = None
        if csv_name not in os.listdir('.'):
            df_generator1 = self.generate_df(batch_nmr=1)
            df_generator1.to_csv(csv_name)
        lists_to_store = [i+1 for i in range(len(os.listdir(self.parsed_json_path)))]
        for i in lists_to_store:
            final_df = pd.read_csv(csv_name)
            df = self.generate_df(i)
            exported_df = pd.concat([df, final_df], sort=False).drop_duplicates()
            exported_df.to_csv(csv_name)
        
        
#l_maker = LargeScaleZapListMaker('', '', 'sao paulo', 'SP', batch_size=10)
#l_maker.organize_and_export_url_list()

#batch_crawler = LSperBatchMinifichaCrawler()
#batch_crawler.store_batch_raw_info(2)

#parser = LSJsonFichaParser()
#parser.parse_and_store_all()

#csv_gen = RealEstateDataCSVgen()
#csv_gen.create_csv()

            
