from bs4 import BeautifulSoup
import concurrent.futures
import sqlite3
import requests
import threading
import queue
import time

BASE_URL = 'https://www.vendr.com'
CATEGORIES = ['DevOps', 'IT Infrastructure', 'Data Analytics and Management']
API_PRODUCT_URL = 'https://www.vendr.com/marketplace/{product_name}?_data=routes%2Fmarketplace.%24companySlug._index'

db_data_queue = queue.Queue()


def get_soup(url) -> BeautifulSoup | None:
    r = requests.get(url)
    if r.status_code != 200:
        return
    soup = BeautifulSoup(r.text, "html.parser")
    return soup


def get_category_url() -> str | None:
    soup = get_soup(BASE_URL)
    if not soup:
        return None
    links = soup.find_all('a', class_='_footer-link_40dln_17')

    for link in links:
        div = link.find('div')
        if div and "Categories" in div.get_text(strip=True):
            url = f"{BASE_URL}{link["href"]}"
            return url
    return None


def get_category_urls(url: str) -> list:
    print('get category urls')
    if not url:
        return []
    links = []
    soup = get_soup(url)
    if not soup:
        print(f"No soup for: {url}")
        return []
    divs = soup.find_all('div', class_='rt-Box rt-r-pb-1')
    for div in divs:
        a = div.find('a')
        if a:
            category_name = a.get_text(strip=True)
            if any(category in category_name for category in CATEGORIES):
                link = f"{BASE_URL}{a["href"]}"
                links.append(link)
    return links


def get_subcategory_urls(urls: list) -> list:
    print('get subcategory urls')
    links_subs = []
    for url in urls:
        soup = get_soup(url)
        if not soup:
            print(f"No soup for: {url}")
            continue
        sub_divs = soup.find_all('div', class_='rt-Box rt-r-pb-1')
        for sub_div in sub_divs:
            a = sub_div.find('a')
            if a:
                sub_category_link = f"{BASE_URL}{a['href']}"
                links_subs.append(sub_category_link)
    return links_subs


def get_product_from_sub_category(url: str) -> list:
    products_slugs = []
    page = 1
    while True:
        full_url = url.replace('page=1', f"page={page}")
        soup = get_soup(full_url)
        if not soup:
            print(f"No soup for:{full_url}")
            break
        products = soup.find_all('a', class_='_card_j928a_9')

        if not products:
            break

        for product in products:
            product_href = product['href']
            product_slug = product_href.split('/')[-1]
            products_slugs.append(product_slug)

        page += 1
        time.sleep(1)

    return products_slugs


def get_main_data(sub_categories_links: list) -> list:
    print('get main data')
    data = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_link = {}

        for link in sub_categories_links:
            future = executor.submit(get_product_from_sub_category, link)
            future_link[future] = link

        for future in concurrent.futures.as_completed(future_link):
            link = future_link[future]
            try:
                products = future.result()
                category_name = link.split('/')[-2]
                sub_category_name = link.split('/')[-1].split('?')[0]

                data.append({
                    'category': category_name.title(),
                    'sub_category': sub_category_name.title(),
                    'products': products
                })

            except Exception as e:
                print(f"Error: {link}-{e}")
    return data


def fetch_product_data(product, catrgory, sub_catrgory, result) -> dict:
    url = API_PRODUCT_URL.format(product_name=product)
    try:
        r = requests.get(url, headers={"Accept": "application/json"}, timeout=15)
        data = r.json()
        company_dict = data.get('company')
        result.put({
            'category': catrgory,
            'sub_category': sub_catrgory,
            'name': company_dict.get('name', 'No name'),
            'average_price': round(float(company_dict.get('stats', {}).get('averageContractValue', 0))),
            'min_price': round(company_dict.get('stats', {}).get('costRange', [0, 0])[0]),
            'max_price': round(company_dict.get('stats', {}).get('costRange', [0, 0])[1]),
            'description': company_dict.get('description', ''),
        })
    except Exception as e:
        print(f"Error: {product}-{e}")
        return {}


def get_all_products_data(data: list) -> queue.Queue:
    print("Get all products data")
    result = queue.Queue()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for d in data:
            category = d['category']
            sub_category = d['sub_category']
            products = d['products']
            for product in products:
                futures.append(executor.submit(fetch_product_data, product, category, sub_category, result))

        concurrent.futures.wait(futures)
    return result


def create_db():
    c = sqlite3.connect('products.db')
    cursor = c.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS products (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        category TEXT,
                        sub_category TEXT,
                        average_price INTEGER,
                        min_price INTEGER,
                        max_price INTEGER,
                        description TEXT)''')
    c.commit()
    c.close()


def save_db(product_data: dict):
    conn = sqlite3.connect('products.db')
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO products (
    name, category, sub_category, average_price, min_price, max_price, description
    ) VALUES (?, ?, ?, ?, ?, ?, ?)''', (
        product_data['name'],
        product_data['category'],
        product_data['sub_category'],
        product_data['average_price'],
        product_data['min_price'],
        product_data['max_price'],
        product_data['description']
    ))
    conn.commit()
    conn.close()


def write_db(result):
    print("Write to db")
    while not result.empty():
        product_data = result.get()
        if product_data is None:
            break
        save_db(product_data)


def main_process():
    print("Start main process")
    url = get_category_url()
    categories_urls = get_category_urls(url)
    sub_links = get_subcategory_urls(categories_urls)
    data = get_main_data(sub_links)
    create_db()
    result = get_all_products_data(data)

    db_thread = threading.Thread(target=write_db, args=(result,))
    db_thread.start()
    db_thread.join()


if __name__ == '__main__':
    main_process()
    print('SUCCESS')
