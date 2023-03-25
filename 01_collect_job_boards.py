# Adding logging to the script
import logging

# Configure logging settings
logging.basicConfig(filename='./logs/01_collect_job_boards.log', filemode='a', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Adding a sqlite3 database to the script
import sqlite3
# create the database and define the schema

import time
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
import pandas as pd

service = EdgeService(executable_path=EdgeChromiumDriverManager().install())
options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

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
        
def generate_urls(JOB, pages_to_scrape=None):
    JOB = JOB.replace(' ', '+')
    if pages_to_scrape is None:
        pages_to_scrape = cacluate_max_pages(JOB) * 10
    else:
        pages_to_scrape = pages_to_scrape * 10
    return [f'https://ca.indeed.com/jobs?q={JOB}&sort=date&start={x}' for x in
                range(0, (pages_to_scrape), 10)]
    
def get_links_from_html(html, search_class='jobsearch-ResultsList', startswith='/rc/clk?jk='):
    """Specific to the Indeed website. Finds the links to the job description pages."""
    soup = BeautifulSoup(html, 'html.parser').find(class_=search_class)
    links = []
    try:
        for link in soup.find_all('a'):
            if link.get('href').startswith(startswith):
                links.append(link.get('href'))
                logging.info(f'Link: {link.get("href")}')
    except Exception as e:
        logging.info(f'Error: {e}')
    return links

def get_html(url):
    """Webdriver to get the html of the page. """
    """This is used to get the links to the job description pages."""
    driver = webdriver.Edge(service=service, options=options)
    logging.info(f'URL call: {url}')
    driver.get(url)
    html = driver.page_source
    driver.close()
    return html

def get_text(url):
    """Webdriver to get the html of the page. """
    """This is used to get the text from the job description pages."""
    driver = webdriver.Edge(service=service, options=options)
    logging.info(f'URL call: {url}')
    driver.get(url)
    page_text = driver.find_element(By.XPATH, "/html/body").text
    driver.close()
    return page_text

def unlist_dataframe(df, column_name):
    df_new = df.explode(column_name).reset_index(drop=True)
    df_new['url'] = [f'https://ca.indeed.com{x}' for x in df_new[column_name]]
        
        
############################################################################################################
# Step 1: Get the links to the job description pages
def step_1__get_job_summary_pages(url, JOB):
    """Webdriver to get the html of the page. """
    """This is used to get the links to the job description pages."""
    # Download the html of the page
    html = get_html(url)
    
    # Extract the links to the job description pages
    job_description_links = get_links_from_html(html)
    df = {
        "job":[JOB for x in job_description_links],
        "urls": [f'https://ca.indeed.com{x}' for x in job_description_links]
    }
    
    db_name = 'step_1__get_job_description_urls'
    db_conn = sqlite3.connect(f'{db_name}.db')
    pd.DataFrame(df).to_sql(db_name, db_conn, if_exists='append', index=False, method='multi', chunksize=500)
    db_conn.close()



# Step 1: (with async) Get the links to the job description pages
async def step_1__async(url, JOB, loop, semaphore):
    async with semaphore:
        with ThreadPoolExecutor() as executor:
            html = await loop.run_in_executor(executor, step_1__get_job_summary_pages, url, JOB)
            return html
        


# Main function
async def main(JOB):
    
    # Create a Semaphore object with a limit of 25
    semaphore = Semaphore(25)
        
    async with aiohttp.ClientSession() as session:
        loop = asyncio.get_event_loop()
        tasks = [step_1__async(url, JOB, loop, semaphore) for url in generate_urls(JOB)]
        pages = await asyncio.gather(*tasks)
        

# Run the main function
if __name__ == "__main__":    
    job_titles = ['Counselor', 'Coach', 'Addiction', 'Social', 'Worker', 'Mental Health']
    for job_title in job_titles:
        logging.info(f'------> Running Main Function with: {job_title}')
        loop = asyncio.get_event_loop()
        task = loop.create_task(main(job_title))
        loop.run_until_complete(asyncio.gather(task))


    # job = 'Coach'
    # url = generate_urls(f'{job}')[0]
    # df = step_1__get_job_summary_pages(url, job)
    # print(job, url, df)
    
    # step_2 = step_2__get_job_description_pages(df)
    # print(step_2, step_2.shape, step_2.columns, step_2.dtypes)