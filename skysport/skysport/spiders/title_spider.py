import scrapy
import json
import pandas as pd
from scrapy import Item, Field
import re
import datetime
import threading
import copy

class Data(Item):
    date = Field()
    tags = Field()
    title = Field()
    text = Field()
    

class MySpider(scrapy.Spider):
    name = "titles"

    def __init__(self):  
        self.year = '2020'
        self.month = '08'
        self.day = '01'
        self.url = str(f'https://sport.sky.it/archivio/{self.year}/{self.month}/{self.day}')


    def start_requests(self):
        
        while(int(self.month) != 7):

            yield scrapy.Request(url=copy.deepcopy(self.url), callback=self.parse_root, cb_kwargs=dict(pag = 1, day = copy.deepcopy(self.day)))
            
            #incrementa giorno
            self.day = ('0' if (int(self.day)+1 < 10) else '') +  str(int(self.day) + 1)
            self.url = str(f'https://sport.sky.it/archivio/{self.year}/{self.month}/{self.day}')
            print("DAY: ",self.day, "MONTH: ",self.month, "YEAR",self.year)
            #controlla validità giorno
            try:
                datetime.datetime(year=int(self.year),month=int(self.month),day=int(self.day))
            except ValueError:
                self.month =  ('0' if (int(self.month)+1 < 10) else '') + str(int(self.month) + 1) #010
                self.day = '01'
                if (int(self.month) > 12):
                    self.month = '01'
                    self.year = str(int(self.year) + 1)
                    


    def parse_root(self, response, pag, day):

        links = response.xpath("//a[contains(@class,'c-card')]//@href").extract()
        links = [link for link in links if "serie-a" in link]
        
        #get numero di pagine
        pages = response.css(".c-pagination > ul > li > a::text").extract()
        pages = [page.replace('\n                    ','') for page in pages]
        pages = [page.replace('\n                ','') for page in pages]
        pages = [page for page in pages if page != '']

        #print('PAGINE')
        #print(day, pages)
        #print(pages[-1])

        #parse_article sui link trovati
        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_article)
        

        #parse sulle next pagine

        for i in range(1,int(pages[-1])):
            pag += 1
            url = response.url + f'?pag={pag}'
            yield scrapy.Request(url=url, callback=self.parse_next_page, cb_kwargs=dict(pag = pag, day = copy.deepcopy(day)) )
            
        

    def parse_next_page(self, response, pag, day):

        links = response.xpath("//a[contains(@class,'c-card')]//@href").extract()
        links = [link for link in links if "serie-a" in link]

        #parse_article sui link trovati
        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_article)


    def parse_article(self, response):

        title = response.xpath("//title//text()").extract_first()
        text = response.xpath("//div[contains(@class,'c-article')]//text()").extract()
        metadata = response.xpath("//script[contains(text(),'window.digitalData')]//text()").extract()
        metadata = ' '.join(metadata).replace('\n', '').replace('   ', '')
        tags = re.search('"pagetag": "(.+?)"', metadata)

        date = re.search('"primapubblicazione": "(.+?)"', metadata)


        data = Data()

        data['date'] = date.group(1)
        data['tags'] = list(tags.group(1).split('|'))
        data['title'] = title
        data['text'] = ' '.join(text).replace('\n', '').replace('   ', '')
     
        yield data


                








