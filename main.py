from largescalecrawlstuff import *

l_maker = LargeScaleZapListMaker('jd bonfiglioli', '', 'sao paulo', 'SP', batch_size=10, chromiun_path='/usr/lib/chromium-browser/chromedriver', path="url_list")
l_maker.organize_and_export_url_list()

batch_crawler = LSperBatchMinifichaCrawler()
batch_crawler.store_all_raw_html()

parser = LSJsonFichaParser(jsonpath="minifichas_list", to_store_path='parsed_fichas')
parser.parse_and_store_all()

csv_gen = RealEstateDataCSVgen()
csv_gen.create_csv()
