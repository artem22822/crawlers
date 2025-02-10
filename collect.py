from playwright.async_api import async_playwright
from multiprocessing import Process, Queue
import aiosqlite
import asyncio
import time
import json

BASE_URL = 'https://books.toscrape.com'


class CrawlerBooks:
    def __init__(self, queue, process_id, cdp_url=None):
        self.queue = queue
        self.process_id = process_id
        self.cdp_url = cdp_url

    async def crawl_book(self, url):
        """Info about the book"""
        async with async_playwright() as ap:
            if self.cdp_url:
                browser = await ap.chromium.connect(self.cdp_url)
            else:
                browser = await ap.chromium.launch(headless=True)
            page = await browser.new_page()
            print(f"(Process {self.process_id}) Scan book: {url}")

            try:
                await page.goto(url)
                breadcrumb_items = await page.query_selector_all('.breadcrumb li')
                category_item = breadcrumb_items[-2]
                title_item = breadcrumb_items[-1]
                category = await category_item.inner_text()
                title = await title_item.inner_text()
                prod_div = page.locator('.product_main')
                price_text = await prod_div.locator('.price_color').text_content()
                price = float(price_text.replace('£', ''))
                in_stock_text = await prod_div.locator('.instock').text_content() or ""
                in_stock_text = in_stock_text.strip()
                in_stock = int(in_stock_text.split('(')[1].split(' ')[0])
                rating_item = await prod_div.locator('.star-rating').get_attribute('class')
                rating = rating_item.split()[-1]
                image_url = await page.locator('#product_gallery img').get_attribute('src')
                image_url = f"{BASE_URL}/{image_url.replace('../../', '')}"
                description_div = page.locator('#product_description')
                if await description_div.count() > 0:
                    description = await description_div.locator('xpath=following-sibling::p').inner_text()
                else:
                    description = ''
                table_item = page.locator('.table-striped')
                if await table_item.count() > 0:
                    rows = await table_item.locator('tr').all()
                else:
                    rows = []
                table_data = {}
                for row in rows:
                    key_elem = await row.locator('th').text_content()
                    value_elem = await row.locator('td').text_content()
                    table_data[key_elem] = value_elem
                book_data = {
                    'category': category,
                    'title': title,
                    'price': price,
                    'in_stock': in_stock,
                    'rating': rating,
                    'image_url': image_url,
                    'description': description,
                    'table_info': json.dumps(table_data)
                }

                await self.save_book(book_data)

            except Exception as e:
                print(f"(Process {self.process_id}) Error scan book {url}: {e}")
                self.queue.put(url)

            await browser.close()

    async def save_book(self, book_data):
        """ Save to DB """
        async with aiosqlite.connect('books.db') as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT,
                    title TEXT,
                    price INTEGER,
                    in_stock INTEGER,
                    rating TEXT,
                    image_url TEXT,
                    description TEXT,
                    table_info TEXT
                )
            """)
            await db.execute("""INSERT INTO books (
                category, title, price, in_stock, rating, image_url, description, table_info)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (
                book_data['category'], book_data['title'], book_data['price'],
                book_data['in_stock'], book_data['rating'], book_data['image_url'],
                book_data['description'], book_data['table_info']
            ))
            await db.commit()

    async def queue_books(self):
        """ Start queue books"""
        while not self.queue.empty():
            url = self.queue.get()
            await self.crawl_book(url)


def run_crawler(queue, process_id, cdp_url=None):
    """ Start crawler functions"""
    crawler = CrawlerBooks(queue, process_id, cdp_url)
    asyncio.run(crawler.queue_books())


class ProcessManager:
    def __init__(self, max_processes=3, cdp_url=None):
        self.max_processes = max_processes
        self.queue = Queue()
        self.processes = []
        self.cdp_url = cdp_url

    async def get_book_links(self):
        """ Get all links from books"""
        print("Start get book links")
        async with async_playwright() as ap:
            browser = await ap.chromium.launch(headless=True)
            page = await browser.new_page()
            url = BASE_URL

            while True:
                await page.goto(url)
                book_elements = await page.query_selector_all('.product_pod h3 a')
                for book in book_elements:
                    book_href = await book.get_attribute('href')
                    if book_href:
                        book_url = f"{BASE_URL}/catalogue/{book_href.replace('catalogue/', '')}"
                        self.queue.put(book_url)

                button_next = await page.query_selector('.next a')
                if button_next:
                    next_href = await button_next.get_attribute('href')
                    next_href = next_href.split("/")[-1]
                    url = f"{BASE_URL}/catalogue/{next_href}"
                else:
                    break

            await browser.close()
            print(f"Books found: {self.queue.qsize()}")

    def start_processes(self):
        """ Start processes """
        for p in range(self.max_processes):
            process = Process(target=run_crawler, args=(self.queue, p, self.cdp_url))
            process.start()
            self.processes.append(process)

    def monitor_processes(self):
        """Monitors processes and restarts failed process"""
        while any(p.is_alive() for p in self.processes):
            for index, process in enumerate(self.processes):
                if not process.is_alive():
                    print(f"(Monitor) process {index} failed! Restart process")
                    process.terminate()
                    new_process = Process(target=run_crawler, args=(self.queue, index, self.cdp_url))
                    new_process.start()
                    self.processes[index] = new_process
            time.sleep(3)

    def run(self):
        """ Main start manager process"""
        asyncio.run(self.get_book_links())
        self.start_processes()
        self.monitor_processes()


if __name__ == "__main__":
    max_processes = int(input('Enter the number of processes ') or 3)
    cdp_url = input('Enter CDP URL (optional) or just press Enter ') or None
    manager = ProcessManager(max_processes=max_processes, cdp_url=cdp_url)
    manager.run()
    print('SUCCESS')
