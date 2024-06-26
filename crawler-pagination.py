import os
import csv
import json
import logging
from urllib.parse import urlencode
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.common.by import By
from dataclasses import dataclass, field, fields, asdict

OPTIONS = webdriver.ChromeOptions()
OPTIONS.add_argument("--headless")

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_search_results(keyword, location, page_number, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.g2.com/search?page={page_number+1}&query={formatted_keyword}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        driver = webdriver.Chrome(options=OPTIONS)
        try:
            driver.get(url)
            logger.info(f"Fetched {url}")
                
            ## Extract Data

            
            div_cards = driver.find_elements(By.CSS_SELECTOR, "div[class='product-listing mb-1 border-bottom']")


            for div_card in div_cards:

                name = div_card.find_element(By.CSS_SELECTOR, "div[class='product-listing__product-name']")

                g2_url = name.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

                rating_elements = div_card.find_elements(By.CSS_SELECTOR, "span[class='fw-semibold']")
                has_rating = len(rating_elements) > 0 
                rating = 0.0

                if has_rating:
                    rating = rating_elements[0].text

                description = div_card.find_element(By.CSS_SELECTOR, "p").text
                
                search_data = {
                    "name": name.text,
                    "stars": rating,
                    "g2_url": g2_url,
                    "description": description
                }
                print(search_data)

            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")

        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def start_scrape(keyword, pages, location, max_threads=5, retries=3):
    for page in range(pages):
        scrape_search_results(keyword, location, page_number, retries=retries)


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["online bank"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        start_scrape(keyword, PAGES, LOCATION, retries=MAX_RETRIES)
    logger.info(f"Crawl complete.")