import scrapy
import xmltodict
import json
from time import strftime, gmtime
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy import Request
import json
import traceback
import re
import os


class TaHospitalSpider(scrapy.Spider):
    name = "ta_hospital"
    allowed_domains = ["tamanhhospital.vn"]
    start_urls = [
        # "https://tamanhhospital.vn/",
        # "https://tamanhhospital.vn/benh-sitemap1.xml",
        # "https://tamanhhospital.vn/benh-sitemap2.xml", # 1296 urls
        # "https://tamanhhospital.vn/thuoc-sitemap.xml", # 243 urls
        # "https://tamanhhospital.vn/cothenguoi-sitemap.xml", # 224 urls
        # "https://tamanhhospital.vn/tiemchung-sitemap.xml", # 171 urls
        # "https://tamanhhospital.vn/vikhuan-sitemap.xml", # 6 urls
        # "https://tamanhhospital.vn/virus-sitemap.xml", # 25 urls
        # "https://tamanhhospital.vn/tebao-sitemap.xml", # 17 urls
        # "https://tamanhhospital.vn/vitamin-sitemap.xml", # 15 urls
        "https://tamanhhospital.vn/hormone-sitemap.xml" # 4 urls
        ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1,           # chờ 1s giữa các request cùng domain
        "RANDOMIZE_DOWNLOAD_DELAY": True, # thêm ngẫu nhiên để tránh bị nhận diện bot
        "CONCURRENT_REQUESTS": 32, 
        "CONCURRENT_REQUESTS_PER_DOMAIN": 16,

        # Cấu hình pipeline
        "ITEM_PIPELINES": {
            "hospital_crawler.pipelines.GoogleDrivePipeline": 300,
        },

        # Google Drive settings
        "GOOGLE_OAUTH_KEY_FILE": "/Users/nhatnamdo/Documents/Workspace/hospital_crawler/hospital_crawler/credentials.json",
        "GOOGLE_OAUTH_TOKEN_FILE": 'token.json',
        "GOOGLE_DRIVE_PARENT_FOLDER_ID": '1LY22CGQ8w1Y8ciZuv46pCQPIiKKiGLfs',  # None để tự tạo folder root


        'LOG_LEVEL': 'INFO'

        }

    def __init__(self):
        super().__init__()
        # self.urls_by_category = {
        #     "benh": [],
        #     "thuoc": [],
        #     "cothenguoi": [],
        #     "tiemchung": [],
        #     "vikhuan": [],
        #     "virus": [],
        #     "tebao": [],
        #     "vitamin": [],
        #     "hormone": []
        # }
        self.visited_urls = set()  # lưu tạm các URL để khi kết thúc sẽ ghi ra file


    def parse(self, response):
        if response.status == 403:
            self.logger.error(f"🚫 Access forbidden for URL: {response.url}")
            return

        self.logger.info(f"📥 Received response from {response.url} ({response.status})")

        try:
            # Parse XML body thành dict
            json_data = xmltodict.parse(response.text)

            # Kiểm tra đây là sitemapindex hay urlset
            if 'sitemapindex' in json_data:
                self.logger.info(f"📋 Found sitemapindex - parsing sub-sitemaps...")
                sitemap_list = json_data['sitemapindex'].get('sitemap', [])
                if isinstance(sitemap_list, dict):
                    sitemap_list = [sitemap_list]

                for sitemap_item in sitemap_list:
                    sitemap_url = sitemap_item.get('loc')
                    if sitemap_url:
                        yield Request(
                            url=sitemap_url,
                            callback=self.parse,
                            headers={'Referer': 'https://tamanhhospital.vn/'},
                            priority=1
                        )

            elif 'urlset' in json_data:
                self.logger.info(f"📄 Found urlset - extracting URLs...")
                url_list = json_data['urlset'].get('url', [])
                if isinstance(url_list, dict):
                    url_list = [url_list]


                extracted_urls = []
                for url_item in url_list:
                    if isinstance(url_item, dict):
                        loc = url_item.get('loc')
                        if loc and loc not in self.visited_urls:
                            extracted_urls.append(loc)
                            self.visited_urls.add(loc)
                            yield Request(
                                url=loc, 
                                callback=self.parse_info,
                                headers={'Referer': 'https://tamanhhospital.vn/'}
                            )

                # Thêm urls theo nhóm
                # category = self.detect_category(response.url)
                # if category:
                #     self.urls_by_category[category].extend(extracted_urls)

                self.logger.info(f"📊 Found {len(extracted_urls)} unique URLs in {response.url}")

            else:
                self.logger.warning(f"⚠️ Unknown sitemap format: {list(json_data.keys())}")

        except Exception as e:
            self.logger.error(f"❌ Error parsing sitemap {response.url}: {e}")
            self.logger.error(f"Response body preview: {response.text[:500]}")
            self.logger.error(traceback.format_exc())


    def parse_info(self, response):
        try:
            print(f'📄 Parsing product: {response.url}')
            url = response.url
            soup = BeautifulSoup(response.text, "lxml")

            detail_container = soup.find("div", id="ftwp-postcontent")
            informations ={}

            if not detail_container:
                print(f"⚠️ Critical: Main 'ftwp-postcontent' container not found for {url}. Aborting.")
                return

            full_info = self.parse_full_info(detail_container, url)
            informations['full_info'] = full_info

            yield {
                'url': url,
                'page_content': response.text,
                'informations': informations,
                "crawled_at": strftime("%Y-%m-%d %H:%M:%S", gmtime()),
                'status': 'success'
            }
        
        except Exception as e:
            print(f'❌ Error parsing article {response.url}: {e}')
            yield {
                'url': url,
                'page_content': response.body,
                'informations': informations,
                "crawled_at": strftime("%Y-%m-%d %H:%M:%S", gmtime()),
                "status": f"error: {str(e)}"
            }


    def parse_full_info(self, detail_container, url):
        """
        Extracts and formats content from <h2>, <h3>, <p>, <ul>, <li> tags.
        - Bold / anchor text inside these tags will be included.
        - h2 sections are separated by blank lines (\n\n).
        - <ul>/<li> items are formatted with "- " prefix.
        """
        allowed_tags = ["h2", "h3", "p","li"]
        document_lines = []
        previous_tag = None

        document_lines.append(str(url))
        document_lines.append(f'Crawled at: {strftime("%Y-%m-%d %H:%M:%S", gmtime())}')

        nav = detail_container.find("nav")
        hospital_info = detail_container.find("div", class_='content_insert')

        # Bỏ đi mục lục
        if nav:
            nav.decompose()
        # Bỏ đi thông tin bệnh viện
        if hospital_info:
            hospital_info.decompose()

        for tag in detail_container.find_all(allowed_tags, recursive=True):
            text = tag.get_text(separator=" ", strip=True)

            # Add blank line before new <h2> section
            if tag.name == "h2" and previous_tag is not None:
                document_lines.append("")  # adds \n\n when joined

            if not text:
                continue

            # Format unordered lists
            if tag.name == "li":
                document_lines.append(f"- {text}")
            else:
                document_lines.append(text)

            previous_tag = tag.name

        formatted_document = "\n".join(document_lines)
        return formatted_document



if __name__ == "__main__":
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "WARNING",  # giảm log rác của Scrapy
    })
    process.crawl(TaHospitalSpider)
    process.start()
    