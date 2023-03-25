import logging

# Configure logging settings
logging.basicConfig(filename='log_file.log', filemode='a', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


import asyncio
from asyncio import Semaphore
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager

service = EdgeService(executable_path=EdgeChromiumDriverManager().install())
options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Edge(service=service, options=options)

def cacluate_max_pages(JOB):
        JOB = JOB.replace(' ', '+')
        url = f'https://ca.indeed.com/jobs?q={JOB}&sort=date&start=10000'
        logging.info(f'URL call: {url}')
        driver = webdriver.Edge(service=service, options=options)
        driver.get(url)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser').find_all(class_='css-tvvxwd')
        driver.close()
        try:
            max_pages = int(soup[len(soup) - 1].text)
            logging.info(f'{JOB} Max Pages: {max_pages}')
            return max_pages
        except Exception as e:
            logging.info(f'{JOB} Produced error -going with default Max Pages: {0}')
            return 0
        
def generate_urls(JOB):
    JOB = JOB.replace(' ', '+')
    pages_to_scrape = cacluate_max_pages(JOB) * 10
    return [f'https://ca.indeed.com/jobs?q={JOB}&sort=date&start={x}' for x in
                range(0, (pages_to_scrape), 10)]
        
# def get_links_from_html(html, search_class='jobsearch-ResultsList', startswith='/rc/clk?jk='):
#     soup = BeautifulSoup(html, 'html.parser').find(class_=search_class)
#     links = []
#     try:
#         for link in soup.find_all('a'):
#             if link.get('href').startswith(startswith):
#                 links.append(link.get('href'))
#     except Exception as e:
#         print(e)
#     return links


def convert_to_hash(file_name):
    """converts a string to a hash"""
    hasher = hashlib.sha256()
    hasher.update(file_name.encode('utf-8'))
    url_hash = hasher.hexdigest()
    return url_hash

# Step 1: Get the links to the job description pages
def step_1__get_job_summary_pages(url, JOB):
    """Webdriver to get the html of the page. """
    """This is used to get the links to the job description pages."""
    # try:
    driver = webdriver.Edge(service=service, options=options)
    driver.get(url)
    html = driver.page_source
    # page_text = driver.find_element(By.XPATH, "/html/body").text
    driver.close()
    dir_name = JOB.replace(' ', '_').lower()
    if not os.path.exists(f'C:\\Projects\\Github\\JobHunter\\text\\{dir_name}_pages'):
        os.makedirs(f'C:\\Projects\\Github\\JobHunter\\text\\{dir_name}_pages')
    with open(f'C:\\Projects\\Github\\JobHunter\\text\\{dir_name}_pages\\{convert_to_hash(url)}.html', 'w+', encoding='utf-8') as f:
        f.write(html)
    logging.info(f'URL call: {url}')
    # return get_links_from_html(html)

    # except Exception as e:
    #     print(e)

# # Step 2: Get the job description pages
# def step_2__get_job_description_pages(urls, JOB):        
#     for page_url in urls:
#         driver = webdriver.Edge(service=service, options=options)
#         driver.get(page_url)
#         logging.info(f'URL call: {page_url}')
#         page_text = driver.find_element(By.XPATH, "/html/body").text
#         dir_name = JOB.replace(' ', '_').lower()
#         logging.log(f'Writing to file: C:\\Projects\\Github\\JobHunter\\text\\{dir_name}\\{convert_to_hash(page_url)}.txt')
#         if not os.path.exists(f'C:\\Projects\\Github\\JobHunter\\text\\{dir_name}_jobs'):
#             os.makedirs(f'C:\\Projects\\Github\\JobHunter\\text\\{dir_name}')
#         with open(f'C:\\Projects\\Github\\JobHunter\\text\\{dir_name}\\{convert_to_hash(page_url)}.txt', 'w+', encoding='utf-8') as f:
#             f.write(page_text)
#         driver.close()


async def step_1__async(url, JOB, loop, semaphore):
    async with semaphore:
        with ThreadPoolExecutor() as executor:
            html = await loop.run_in_executor(executor, step_1__get_job_summary_pages, url, JOB)
            return html
        
# async def step_2__async(urls, JOB, loop, semaphore):
#     async with semaphore:
#         with ThreadPoolExecutor() as executor:
#             html = await loop.run_in_executor(executor, step_2__get_job_description_pages, urls, JOB)
#             return html

# Main function
async def main(JOB):
    
    # Create a Semaphore object with a limit of 25
    semaphore = Semaphore(25)
        
    async with aiohttp.ClientSession() as session:
        loop = asyncio.get_event_loop()
        tasks = [step_1__async(url, JOB, loop, semaphore) for url in generate_urls(JOB)]
        list_of_urls = await asyncio.gather(*tasks)
        logging.info(f'List of URLs: {list_of_urls}')   
    # async with aiohttp.ClientSession() as session:
    #     loop = asyncio.get_event_loop()
    #     tasks = [step_2__async(urls, JOB, loop, semaphore) for urls in list_of_urls]
    #     job_description_text = await asyncio.gather(*tasks)


# Run the main function
if __name__ == "__main__":
    for job_title in ['Coach', 'Counselor', 'Addiction', 'Social', 'Worker', 'Mental Health', 'Mental Health Worker', 'Mental Health Counselor', 'Mental Health Coach']:
        logging.info(f'------> Running Main Function with: {job_title}')
        asyncio.run(
            main(
                JOB=job_title
            )
        )
