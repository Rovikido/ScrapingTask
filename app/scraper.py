import json
import logging
import aiohttp
import asyncio

from app.data_extractor import ContractorDataExtractor


class ContractorDataFetcher:
    def __init__(self, base_url, headers, params, items_per_page=15, rate_limit_per_s=10, max_retries=3, backoff_factor=5, logger=None):
        """
        Initializes the ContractorDataFetcher with required configurations.
        
        Args:
            base_url (str): The base URL for the paginated requests.
            headers (dict): The headers to be used in the requests.
            params (dict): The query parameters to be used in the requests.
            items_per_page (int): The number of items per page.
            rate_limit_per_s (int): Max number of requests per second
            logger (logging.Logger): Optional logger to log the process.
            max_retries (int, optional): Maximum number of retries before failing. Default is 3.
            backoff_factor (float, optional): Factor to multiply the sleep time with after each retry. Default is 5.
        """
        self.base_url = base_url
        self.items_per_page = items_per_page
        self.headers = headers
        self.params = params
        self.logger = logger or logging.getLogger(__name__)
        self.requests_per_batch = rate_limit_per_s


    async def fetch_page(self, session, url):
        """Fetch a page of contractor data with retries in case of failure."""
        attempt = 0
        while attempt < self.max_retries:
            try:
                async with session.get(url, headers=self.headers, params=self.params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        self.logger.error(f"Failed to fetch {url} - Status: {response.status}")
                        return {}
            except Exception as e:
                attempt += 1
                if attempt < self.max_retries:
                    sleep_time = self.backoff_factor ** attempt
                    self.logger.warning(f"Error fetching {url}: {e}. Retrying in {sleep_time} seconds... (Attempt {attempt}/{self.max_retries})")
                    await asyncio.sleep(sleep_time)
                else:
                    self.logger.error(f"Error fetching {url}: {e}. Max retries reached.")
                    return {}

    async def fetch_all_data(self, total_items, extractor: ContractorDataExtractor):
        """Fetch all paginated contractor data, divided into batches."""
        tasks = []
        all_data = {}

        async with aiohttp.ClientSession() as session:
            # Loop through the total number of items, split into batches
            for page in range(0, total_items, self.items_per_page * self.requests_per_batch):
                batch_start = page
                batch_end = min(page + self.items_per_page * self.requests_per_batch, total_items)

                self.logger.info(f"Fetching batch from page {batch_start} to {batch_end}")

                # Create tasks for the current batch
                for page_number in range(batch_start, batch_end, self.items_per_page):
                    page_url = self.form_page_url(page_number)
                    self.logger.info(f"Fetching {page_number} with URL {page_url}")
                    tasks.append(self.fetch_page(session, page_url))

                # Gather responses for the current batch
                responses = await asyncio.gather(*tasks)
                tasks = []  # Reset tasks for the next batch

                # Process the responses and update all_data
                for response in responses:
                    if response:
                        extracted_data = extractor.extract_data(response)
                        all_data.update(extracted_data)

                await asyncio.sleep(1)

        return {"contractor_data": all_data}

    def form_page_url(self, page_number):
        """Forms the page URL based on the page number."""
        return f"{self.base_url}?fi={page_number}"