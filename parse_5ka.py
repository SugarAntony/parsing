import json
import time
import requests


class Parse5Ka:
    params = {
        'records_per_page': 100,
        'categories': 0
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0'
    }

    def __init__(self, url_main, url_cat):
        self.url_main = url_main
        self.url_cat = url_cat

    def parse(self):
        url_cat = self.url_cat

        params = self.params
        headers = self.headers

        response_cat: requests.Response = requests.get(url_cat, headers=headers)

        while response_cat.status_code != 200:
            time.sleep(0.02)
            response_cat: requests.Response = requests.get(url_cat, headers=headers)

        data_cat = response_cat.json()
        for cat in data_cat:
            url_main = self.url_main
            cat['products'] = []

            params['categories'] = cat['parent_group_code']

            while url_main:
                response: requests.Response = requests.get(url_main, params=params, headers=headers)
                while response.status_code != 200:
                    time.sleep(0.02)
                    response: requests.Response = requests.get(url_main,  params=params, headers=headers)

                data = response.json()
                for product in data.get('results', []):
                    cat['products'].append(product)
                self.save_categories(cat)

                url_main = data.get('next')
                if params:
                    params = {}

    def save_categories(self, category: dict):
        with open(f'categories/{category["parent_group_name"]}.json', 'w', encoding='UTF-8') as file:
            json.dump(category, file, ensure_ascii=False)


if __name__ == '__main__':
    url = 'https://5ka.ru/api/v2/special_offers'
    url_categories = 'https://5ka.ru/api/v2/categories/'
    parser = Parse5Ka(url, url_categories)
    parser.parse()
