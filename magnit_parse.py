import time
import datetime
import os
import dotenv
import requests
from urllib.parse import urljoin
import bs4
import pymongo as pm

dotenv.load_dotenv('.env')


class MagnitParser:

    months_dict = {
        'января': 1,
        'февраля': 2,
        'марта': 3,
        'апреля': 4,
        'мая': 5,
        'июня': 6,
        'июля': 7,
        'августа': 8,
        'сентября': 9,
        'октября': 10,
        'ноября': 11,
        'декабря': 12
    }

    def __init__(self, start_url):
        self.start_url = start_url
        mongo_client = pm.MongoClient(os.getenv('DATA_BASE'))
        self.db = mongo_client['parse_magnit']

    def _get(self, url: str) -> bs4.BeautifulSoup:
        response: requests.Response = requests.get(url)
        while response.status_code != 200:
            time.sleep(0.25)
            response: requests.Response = requests.get(url)
        return bs4.BeautifulSoup(response.text, 'lxml')

    def run(self):
        soup = self._get(self.start_url)
        for product in self.parse(soup):
            self.save(product)

    def parse(self, soup: bs4.BeautifulSoup) -> dict:
        catalog = soup.find('div', attrs={'class': 'сatalogue__main'})
        pr_data = {}

        for product in catalog.findChildren('a'):
            try:
                old_price_integer = product.find('div', attrs={'class', 'label__price_old'}).find('span',
                                                                attrs={'class', 'label__price-integer'}).text
                old_price_decimal = product.find('div', attrs={'class', 'label__price_old'}).find('span',
                                                                attrs={'class', 'label__price-decimal'}).text
                old_price = float(old_price_integer + '.' + old_price_decimal)

                new_price_integer = product.find('div', attrs={'class', 'label__price_new'}).find('span',
                                                                attrs={'class', 'label__price-integer'}).text
                new_price_decimal = product.find('div', attrs={'class', 'label__price_new'}).find('span',
                                                                attrs={'class', 'label__price-decimal'}).text
                new_price = float(new_price_integer + '.' + new_price_decimal)

                current_year = datetime.datetime.now().year
                dates: list = product.find('div', attrs={'class', 'card-sale__date'}).findChildren('p')
                date_from = dates[0].text if len(dates) > 0 else datetime.date(1, 1, 1)
                date_to = dates[1].text if len(dates) > 1 else datetime.date(1, 1, 1)

                if date_from != datetime.date(1, 1, 1):
                    date_from_month = self.months_dict.get(date_from[5:])
                    date_from_day = int(date_from[2:4])
                    current_year = current_year
                    date_from = datetime.date(year=current_year, month=date_from_month, day=date_from_day)

                    if date_to != datetime.date(1, 1, 1):
                        date_to_month = self.months_dict.get(date_to[6:])
                        date_to_day = int(date_to[3:5])
                        current_year = current_year if int(date_to_month) > date_from_month else current_year + 1
                        date_to = datetime.date(year=current_year, month=date_to_month, day=date_to_day)

                pr_data = {
                    'url': urljoin(self.start_url, product.attrs.get('href')),
                    'promo_name': product.find('div', attrs={'class', 'card-sale__header'}).text,
                    'product_name': product.find('div', attrs={'class', 'card-sale__title'}).text,
                    'old_price': old_price,
                    'new_price': new_price,
                    'image_url': product.find('img')['data-src'],
                    'date_from': str(date_from),
                    'date_to': str(date_to)
                    }
            except AttributeError:
                continue

            yield pr_data

    def save(self, data: dict):
        collection = self.db['parse_magnit']
        collection.insert_one(data)


if __name__ == '__main__':
    url = 'https://magnit.ru/promo/?geo=moskva'
    parser = MagnitParser(url)
    parser.run()