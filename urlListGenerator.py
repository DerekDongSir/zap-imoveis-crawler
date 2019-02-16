from bs4 import BeautifulSoup
import re
import json
from selenium import webdriver

class URL_list_generator:
    def __init__(self, bairro='', zona='', cidade='', estado='', chromiun_path='/usr/lib/chromium-browser/chromedriver'):
        self.url_base = 'https://www.zapimoveis.com.br/venda/apartamentos/'
        self.url_mid = estado + '+' + cidade + '+' + zona + '+' + bairro + '/#'
        self.url_params = {"precomaximo":"2147483647",
                           "possuiendereco":"True",
                           "parametrosautosuggest":[{"Bairro":bairro,
                                                     "Zona":"",
                                                     "Cidade":cidade,
                                                     "Agrupamento":zona,
                                                     "Estado":estado.upper()}],"pagina":"1","ordem":"Relevancia","paginaOrigem":"ResultadoBusca","semente":"1741983183","formato":"Lista"}
        self.url_parent = self.url_base + self.url_mid +json.dumps(self.url_params)
        self.chromiun_path = chromiun_path

    def get_page_number(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        driver = webdriver.Chrome(self.chromiun_path, chrome_options=chrome_options)
        driver.get(self.url_parent)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        try:
            page_len_text = soup.find('span', {'class':'pull-right num-of'}).text.replace(".", "")
            nmr = re.findall('\d+', page_len_text)[0]
        except:
            nmr = 1
        driver.close()
        return int(nmr)
    
    def get_url_list(self):
        page_nmr = self.get_page_number()
        base = self.url_base + self.url_mid
        params = self.url_params
        url_list = []
        for i in range(1, page_nmr+1):
            params['pagina'] = i
            url_to_parse = base + json.dumps(params)
            url_list.append(url_to_parse)
        return url_list