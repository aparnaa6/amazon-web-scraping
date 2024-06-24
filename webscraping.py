import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import time

class WebScraper:
    def __init__(self, start_url, output_file, max_pages=5, concurrency=10, max_timeout=10, max_retry=3):
        self.start_url = start_url
        self.output_file = output_file
        self.max_pages = max_pages
        self.concurrency = concurrency
        self.max_timeout = max_timeout
        self.max_retry = max_retry
        self.domain = urlparse(start_url).netloc
        self.visited_urls = set()
        self.queue = deque([start_url])
        self.results = []

    def fetch(self, url):
        try:
            start_time = time.time()
            response = requests.get(url, timeout=self.max_timeout)
            end_time = time.time()
            download_time = end_time - start_time

            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            title = soup.title.string if soup.title else '[]'
            h1_text = soup.find('h1').text if soup.find('h1') else '[]'
            h2_text = soup.find('h2').text if soup.find('h2') else '[]'
            size = len(html)
            response_code = response.status_code

            self.results.append({
                'url': url,
                'title': title,
                'h1_text': h1_text,
                'h2_text': h2_text,
                'size': size,
                'response_code': response_code,
                'download_time': download_time
            })

            links = soup.find_all('a', href=True)
            for link in links:
                absolute_link = urljoin(url, link['href'])
                if urlparse(absolute_link).netloc == self.domain:
                    self.queue.append(absolute_link)

        except Exception as e:
            print(f"Error fetching URL {url}: {e}")

    def process_queue(self):
        while len(self.results) < self.max_pages and self.queue:
            url = self.queue.popleft()
            if url not in self.visited_urls and urlparse(url).netloc == self.domain:
                self.visited_urls.add(url)
                self.fetch(url)

    def crawl(self):
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            while len(self.results) < self.max_pages and self.queue:
                futures = [executor.submit(self.process_queue) for _ in range(self.concurrency)]
                for future in futures:
                    future.result()

    def write_to_csv(self):
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['url', 'title', 'h1_text', 'h2_text', 'size', 'response_code', 'download_time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in self.results:
                writer.writerow(result)

    def run(self):
        self.crawl()
        self.write_to_csv()
        return self.results

if __name__ == "__main__":
    start_url = "https://www.amazon.in/"
    output_file = "data_scraped.csv"
    scraper = WebScraper(start_url, output_file)
    results = scraper.run()
    print("Scraped data saved to", output_file)
