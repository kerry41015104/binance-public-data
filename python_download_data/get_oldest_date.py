#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
優化的幣安數據爬蟲 - 專門從ZIP文件名提取日期
基於高級爬蟲策略，專門針對ZIP文件名中的日期進行提取
目標：從 UNIUSDC-1m-2025-06-16.zip 提取 2025-06-16
"""

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, date
import time
import logging
import json
import random
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


class OptimizedBinanceDataScraper:
    """優化的幣安數據頁面爬蟲類 - 專門提取ZIP文件名中的日期"""

    def __init__(self, timeout=30, retry_count=3):
        self.timeout = timeout
        self.retry_count = retry_count
        self.session = requests.Session()

        # 隨機化用戶代理
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]

        self._update_headers()

    def _update_headers(self):
        """更新請求頭"""
        self.session.headers.update(
            {
                "User-Agent": random.choice(self.user_agents),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0",
            }
        )

    def get_earliest_date_from_url(self, url, use_alternative_methods=True):
        """
        從指定URL獲取最早的ZIP文件日期，支持多種方法

        Args:
            url (str): 幣安數據頁面URL
            use_alternative_methods (bool): 是否使用備用方法

        Returns:
            date or None: 最早日期，失敗返回None
        """
        logger.info(f"開始爬取頁面: {url}")

        # # 方法1: 直接HTML解析
        # result = self._try_html_parsing(url)
        # if result:
        #     return result

        # if not use_alternative_methods:
        #     return None

        # 嘗試API端點，有效方法
        result = self._try_api_endpoint(url)
        if result:
            return result

        # # 方法3: 嘗試不同的URL格式
        # result = self._try_alternative_urls(url)
        # if result:
        #     return result

        # # 方法4: 模擬瀏覽器行為
        # result = self._try_browser_simulation(url)
        # if result:
        #     return result

        return None

    def _try_html_parsing(self, url):
        """嘗試直接HTML解析"""
        for attempt in range(self.retry_count):
            try:
                # 隨機延遲，模擬人類行為
                time.sleep(random.uniform(1, 3))

                # 更新請求頭
                self._update_headers()

                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()

                # 檢查是否被重定向到驗證頁面
                if "challenge" in response.url or "captcha" in response.text.lower():
                    logger.warning("檢測到驗證頁面，可能被反爬蟲系統攔截")
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                dates = self._extract_dates_from_zip_files_only(soup, response.text)

                if dates:
                    return min(dates)

            except Exception as e:
                logger.error(
                    f"HTML解析失敗 (嘗試 {attempt + 1}/{self.retry_count}): {str(e)}"
                )
                if attempt < self.retry_count - 1:
                    time.sleep(2**attempt)

        return None

    def _try_api_endpoint(self, url):
        """嘗試查找API端點"""
        try:
            # 嘗試構建可能的API URL
            if "data.binance.vision" in url:
                # 提取路徑信息
                path_match = re.search(r"prefix=(.+)", url)
                if path_match:
                    path = path_match.group(1)
                    api_url = f"https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/{path}"

                    logger.info(f"嘗試API端點: {api_url}")
                    response = self.session.get(api_url, timeout=self.timeout)

                    if response.status_code == 200:
                        # 嘗試解析XML響應中的ZIP文件
                        dates = self._extract_dates_from_xml_zip_files(response.text)
                        if dates:
                            return min(dates)

        except Exception as e:
            logger.debug(f"API端點嘗試失敗: {str(e)}")

        return None

    def _try_alternative_urls(self, url):
        """嘗試不同的URL格式"""
        alternative_urls = []

        # 移除/添加尾隨斜杠
        if url.endswith("/"):
            alternative_urls.append(url[:-1])
        else:
            alternative_urls.append(url + "/")

        # 嘗試不同的參數格式
        if "?" in url:
            base_url = url.split("?")[0]
            params = url.split("?")[1]

            # 嘗試不同的參數編碼
            alternative_urls.extend(
                [
                    f"{base_url}?{params.replace('/', '%2F')}",
                    f"{base_url}#{params}",
                    base_url,
                ]
            )

        for alt_url in alternative_urls:
            logger.info(f"嘗試備用URL: {alt_url}")
            result = self._try_html_parsing(alt_url)
            if result:
                return result

        return None

    def _try_browser_simulation(self, url):
        """模擬瀏覽器行為"""
        try:
            # 先訪問主頁建立session
            logger.info("模擬瀏覽器：先訪問主頁")
            self.session.get("https://data.binance.vision/", timeout=self.timeout)

            # 添加referer頭
            self.session.headers.update({"Referer": "https://data.binance.vision/"})

            # 模擬用戶點擊行為的延遲
            time.sleep(random.uniform(2, 5))

            # 再次嘗試目標URL
            return self._try_html_parsing(url)

        except Exception as e:
            logger.debug(f"瀏覽器模擬失敗: {str(e)}")

        return None

    def _extract_dates_from_zip_files_only(self, soup, raw_text):
        """專門從ZIP文件名中提取日期"""
        dates = []

        # 方法1: 從HTML表格中的ZIP文件連結提取
        table_dates = self._extract_zip_dates_from_table(soup)
        dates.extend(table_dates)

        # 方法2: 從所有ZIP文件連結提取
        link_dates = self._extract_zip_dates_from_links(soup)
        dates.extend(link_dates)

        # 方法3: 從JavaScript中的ZIP文件名提取
        js_dates = self._extract_zip_dates_from_javascript(raw_text)
        dates.extend(js_dates)

        # 方法4: 從純文本中的ZIP文件名提取
        text_dates = self._extract_zip_dates_from_text(raw_text)
        dates.extend(text_dates)

        # 去重並排序
        unique_dates = sorted(list(set(dates)))
        logger.info(f"從ZIP文件名總共提取到 {len(unique_dates)} 個唯一日期")

        return unique_dates

    def _extract_zip_dates_from_table(self, soup):
        """從表格中的ZIP文件連結提取日期"""
        dates = []
        tables = soup.find_all("table")

        logger.info(f"在表格中查找ZIP文件，找到 {len(tables)} 個表格")

        for table in tables:
            rows = table.find_all("tr")

            for row in rows:
                cells = row.find_all(["td", "th"])

                for cell in cells:
                    # 檢查ZIP文件連結
                    links = cell.find_all("a")
                    for link in links:
                        href = link.get("href", "")
                        text = link.get_text(strip=True)

                        # 只處理ZIP文件，排除CHECKSUM文件
                        if text.endswith(".zip") and not text.endswith(".CHECKSUM"):
                            date_obj = self._extract_date_from_zip_filename(text)
                            if date_obj:
                                dates.append(date_obj)
                                logger.debug(
                                    f"從表格ZIP文件 '{text}' 提取日期: {date_obj}"
                                )

        logger.info(f"從表格中找到 {len(dates)} 個ZIP文件日期")
        return dates

    def _extract_zip_dates_from_links(self, soup):
        """從所有ZIP文件連結中提取日期"""
        dates = []
        links = soup.find_all("a")

        zip_count = 0
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)

            # 檢查連結文本和href是否包含ZIP文件
            for content in [text, href]:
                if content.endswith(".zip") and not content.endswith(".CHECKSUM"):
                    zip_count += 1
                    # 從文件名提取日期
                    filename = content.split("/")[-1] if "/" in content else content
                    date_obj = self._extract_date_from_zip_filename(filename)
                    if date_obj:
                        dates.append(date_obj)
                        logger.debug(f"從連結ZIP文件 '{filename}' 提取日期: {date_obj}")

        logger.info(f"檢查了 {zip_count} 個ZIP文件連結，提取到 {len(dates)} 個日期")
        return dates

    def _extract_zip_dates_from_javascript(self, raw_text):
        """從JavaScript代碼中的ZIP文件名提取日期"""
        dates = []

        try:
            # 查找包含ZIP文件的JavaScript內容
            zip_patterns = [
                r'["\']([^"\']*\.zip)["\']',  # "filename.zip" 或 'filename.zip'
                r'href=["\']([^"\']*\.zip)["\']',  # href="filename.zip"
                r'url:["\']([^"\']*\.zip)["\']',  # url:"filename.zip"
            ]

            for pattern in zip_patterns:
                matches = re.findall(pattern, raw_text)
                for match in matches:
                    if not match.endswith(".CHECKSUM"):
                        filename = match.split("/")[-1] if "/" in match else match
                        date_obj = self._extract_date_from_zip_filename(filename)
                        if date_obj:
                            dates.append(date_obj)
                            logger.debug(
                                f"從JS ZIP文件 '{filename}' 提取日期: {date_obj}"
                            )

        except Exception as e:
            logger.debug(f"JavaScript ZIP文件日期提取失敗: {str(e)}")

        return dates

    def _extract_zip_dates_from_text(self, text):
        """從純文本中使用正則表達式提取ZIP文件名中的日期"""
        dates = []

        try:
            # 匹配ZIP文件名模式
            zip_file_patterns = [
                r"([A-Z0-9]+(?:-[A-Z0-9]+)*-\d+[mhd]-\d{4}-\d{2}-\d{2}\.zip)",  # UNIUSDC-1m-2025-06-16.zip
                r"(\w+-\w+-\d{4}-\d{2}-\d{2}\.zip)",  # symbol-interval-YYYY-MM-DD.zip
                r"(\w+_\w+_\d{4}-\d{2}-\d{2}\.zip)",  # symbol_interval_YYYY-MM-DD.zip
            ]

            for pattern in zip_file_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    if not match.endswith(".CHECKSUM"):
                        date_obj = self._extract_date_from_zip_filename(match)
                        if date_obj:
                            dates.append(date_obj)
                            logger.debug(
                                f"從文本ZIP文件 '{match}' 提取日期: {date_obj}"
                            )

        except Exception as e:
            logger.debug(f"文本ZIP文件日期提取失敗: {str(e)}")

        return dates

    def _extract_dates_from_xml_zip_files(self, xml_text):
        """從XML響應中的ZIP文件名提取日期"""
        dates = []

        try:
            # 查找XML中的Key標籤，包含ZIP文件路徑
            key_patterns = [
                r"<Key>([^<]*\.zip)</Key>",  # <Key>path/filename.zip</Key>
            ]

            for pattern in key_patterns:
                matches = re.findall(pattern, xml_text)
                for match in matches:
                    if not match.endswith(".CHECKSUM"):
                        filename = match.split("/")[-1] if "/" in match else match
                        date_obj = self._extract_date_from_zip_filename(filename)
                        if date_obj:
                            dates.append(date_obj)
                            logger.debug(
                                f"從XML ZIP文件 '{filename}' 提取日期: {date_obj}"
                            )

        except Exception as e:
            logger.debug(f"XML ZIP文件日期提取失敗: {str(e)}")

        return dates

    def _extract_date_from_zip_filename(self, filename):
        """
        專門從ZIP文件名中提取日期

        支持格式：
        - UNIUSDC-1m-2025-06-16.zip
        - BTCUSDT-1h-2025-08-25.zip
        - 等等...
        """
        if not filename or not filename.endswith(".zip"):
            return None

        try:
            # 使用更精確的模式匹配幣安ZIP文件名中的日期
            patterns = [
                r"-(\d{4}-\d{2}-\d{2})\.zip$",  # -YYYY-MM-DD.zip (最常見)
                r"_(\d{4}-\d{2}-\d{2})\.zip$",  # _YYYY-MM-DD.zip
                r"(\d{4}-\d{2}-\d{2})\.zip$",  # YYYY-MM-DD.zip
                r"-(\d{4}-\d{2}-\d{2})-",  # -YYYY-MM-DD-
                r"_(\d{4}-\d{2}-\d{2})_",  # _YYYY-MM-DD_
            ]

            for pattern in patterns:
                match = re.search(pattern, filename)
                if match:
                    date_str = match.group(1)
                    try:
                        # 驗證並轉換日期
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                        # 合理性檢查
                        if 2010 <= date_obj.year <= datetime.now().year + 1:
                            return date_obj
                    except ValueError:
                        continue

        except Exception as e:
            logger.debug(f"從ZIP文件名提取日期失敗 '{filename}': {str(e)}")

        return None

    def get_earliest_date_for_symbol(
        self, trading_type, data_type, symbol, interval=None
    ):
        """獲取特定交易對的最早ZIP文件日期"""
        url = self._build_binance_data_url(trading_type, data_type, symbol, interval)
        if not url:
            return None
        return self.get_earliest_date_from_url(url)

    def _build_binance_data_url(self, trading_type, data_type, symbol, interval=None):
        """構建幣安數據頁面URL"""
        base_url = "https://data.binance.vision"

        try:
            if trading_type == "spot":
                type_path = "?prefix=data/spot"
            elif trading_type == "um":
                type_path = "?prefix=data/futures/um"
            elif trading_type == "cm":
                type_path = "?prefix=data/futures/cm"
            else:
                logger.error(f"不支持的交易類型: {trading_type}")
                return None

            if interval and data_type in [
                "klines",
                "indexPriceKlines",
                "markPriceKlines",
                "premiumIndexKlines",
            ]:
                url = f"{base_url}/?prefix={type_path}/daily/{data_type}/{symbol.upper()}/{interval}/"
            else:
                url = f"{base_url}/?prefix={type_path}/daily/{data_type}/{symbol.upper()}/"

            return url

        except Exception as e:
            logger.error(f"構建URL失敗: {str(e)}")
            return None

    def batch_get_earliest_dates(self, symbol_configs):
        """批量獲取多個交易對的最早ZIP文件日期"""
        results = {}

        for i, config in enumerate(symbol_configs):
            logger.info(f"處理 {i+1}/{len(symbol_configs)}: {config}")

            try:
                earliest_date = self.get_earliest_date_for_symbol(**config)

                # 生成結果鍵
                key = (
                    f"{config['trading_type']}_{config['data_type']}_{config['symbol']}"
                )
                if config.get("interval"):
                    key += f"_{config['interval']}"

                results[key] = earliest_date
                logger.info(f"結果: {key} -> {earliest_date}")

                # 添加延遲
                time.sleep(1)

            except Exception as e:
                logger.error(f"處理配置失敗 {config}: {str(e)}")
                continue

        return results

    def close(self):
        """關閉會話"""
        if self.session:
            self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def demo_usage():
    """示例用法"""
    print("=== 優化的幣安ZIP文件日期爬蟲示例 ===\n")

    # 設置日誌級別
    logging.basicConfig(level=logging.INFO)

    with OptimizedBinanceDataScraper() as scraper:

        # 單個查詢示例
        print("1. 獲取BIOUSDT 1m K線最早ZIP文件日期:")
        earliest_date = scraper.get_earliest_date_for_symbol(
            trading_type="um", data_type="klines", symbol="BIOUSDC", interval="1m"
        )
        print(f"   結果: {earliest_date}\n")

        # # 批量查詢示例
        # print("2. 批量查詢示例:")
        # symbol_configs = [
        #     {
        #         "trading_type": "um",
        #         "data_type": "klines",
        #         "symbol": "BTCUSDT",
        #         "interval": "1h",
        #     },
        #     {
        #         "trading_type": "um",
        #         "data_type": "klines",
        #         "symbol": "ETHUSDT",
        #         "interval": "1h",
        #     },
        # ]

        # results = scraper.batch_get_earliest_dates(symbol_configs)
        # print("   批量查詢結果:")
        # for key, date_value in results.items():
        #     print(f"   {key}: {date_value}")


if __name__ == "__main__":
    start_time = time.time()
    demo_usage()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"總耗時: {elapsed_time:.2f}秒")
