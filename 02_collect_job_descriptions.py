
# Adding logging to the script
import logging
logging.basicConfig(filename='./logs/03_collect_job_descriptions.log', filemode='a', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Adding a sqlite3 database to the script
import sqlite3
import pandas as pd

# Asyncio
import asyncio
from asyncio import Semaphore
import aiohttp
from concurrent.futures import ThreadPoolExecutor

# Web Scraping
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager
service = EdgeService(executable_path=EdgeChromiumDriverManager().install())
options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

def get_text(url):
    """Webdriver to get the html of the page. """
    """This is used to get the text from the job description pages."""
    driver = webdriver.Edge(service=service, options=options)
    logging.info(f'URL call: {url}')
    driver.get(url)
    page_text = driver.find_element(By.XPATH, "/html/body").text
    driver.close()
    return page_text

def df_from_db(db_name, table_name):
    # connect to the database and read the data into a DataFrame
    with sqlite3.connect(f'{db_name}.db') as conn:
        df = pd.read_sql_query(f"SELECT distinct * from {table_name}", conn).drop_duplicates(subset=['urls']).reset_index(drop=True)
    return df

def step_2__get_job_description_pages(df):
    """Webdriver to get the html of the page. """
    # checks if a data type is a list
    if isinstance(df, list):
        df = df[0]

    full_df = pd.DataFrame()
    for index, row in df.iterrows():
        job = row['job']
        url = row['urls']
        # Download the html of the page
        page_text = get_text(url)
        df = {
            "job":[job],
            "urls": [url],
            "page_text": [page_text]
        }
        full_df = pd.concat([full_df,pd.DataFrame(df)], axis=0)
    
        # Insert the data into the database
        db_name = 'step_2__text_from_job_description_page'
        db_conn = sqlite3.connect(f'{db_name}.db')
        pd.DataFrame(df).to_sql(db_name, db_conn, if_exists='append', index=False, method='multi', chunksize=500)
        db_conn.close()
        
        return full_df.reset_index(drop=True)
    
# Step 2: (with async) Get the text from the job description pages
async def step_2__async(df, loop, semaphore):
    async with semaphore:
        with ThreadPoolExecutor() as executor:
            html = await loop.run_in_executor(executor, step_2__get_job_description_pages, df)
            return html

async def main():
    # create a semaphore to limit the number of concurrent requests
    semaphore = Semaphore(25)
    # read the data from the database
    df = df_from_db('step_1__get_job_description_urls', 'step_1__get_job_description_urls')
    async with aiohttp.ClientSession() as session:
        loop = asyncio.get_event_loop()
        # create a list of tasks to run asynchronously
        tasks = [step_2__async(df.loc[[index]], loop, semaphore) for index in range(len(df))]
        # run the tasks concurrently
        results = await asyncio.gather(*tasks)
    # combine the results into a single DataFrame
    full_df = pd.concat(results, axis=0)
    # write the data to the database
    db_name = 'step_2__text_from_job_description_page'
    with sqlite3.connect(f'{db_name}.db') as conn:
        full_df.to_sql(db_name, conn, if_exists='append', index=False, method='multi', chunksize=500)
    # close the session
    await session.close()

# run the main function
asyncio.run(main())




