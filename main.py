import json
import logging
import asyncio
from dotenv import load_dotenv
import os

from app.data_extractor import ContractorDataExtractor
from app.scraper import ContractorDataFetcher


def main():
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    
    cookie = os.getenv('COOKIE')

    base_url = 'https://www.houzz.com/professionals/general-contractor/new-york-city-ny-us-probr0-bo~t_11786~r_5128581'
    headers = {
        'User-agent': 'Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.188 Safari/537.36 CrKey/1.54.250320',
        'Accept': '*/*',
        'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Referer': 'https://www.houzz.com/professionals/general-contractor/new-york-city-ny-us-probr0-bo~t_11786~r_5128581?fi=30',
        'X-Requested-With': 'XMLHttpRequest',
        'x-hz-request': 'true',
        'x-hz-spf-request': 'true',
        'X-SPF-Referer': 'https://www.houzz.com/professionals/general-contractor/new-york-city-ny-us-probr0-bo~t_11786~r_5128581?fi=30',
        'X-SPF-Previous': 'https://www.houzz.com/professionals/general-contractor/new-york-city-ny-us-probr0-bo~t_11786~r_5128581?fi=30',
        'Connection': 'keep-alive',
        'cookie': cookie,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Priority': 'u=0',
        'TE': 'trailers',
    }

    params = {'spf': 'navigate'}

    fields_to_extract = {
        "contractor_fields": ["professionalId", "formattedPhone", "budgetLevels", "numReviews"],
        "location_fields": ["location", "address", "city", "state", "zip", "country", "latitude", "longitude"],
        "contractor_user_fields": ["displayName", "houzzLink", "socialLinks"]
    }
    extractor = ContractorDataExtractor(fields_to_extract=fields_to_extract)

    fetcher = ContractorDataFetcher(base_url, headers, params, rate_limit_per_s=20)

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(fetcher.fetch_all_data(1000, extractor))

    with open("result.json", "w+") as file: 
        json.dump(result, file)


if __name__ == "__main__":
    main()