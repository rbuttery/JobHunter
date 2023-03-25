import asyncio
import os
import time
import openai
from datetime import date
import pandas as pd
import json

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# OpenAI API Key
with open(r"C:\Users\rober\.openai\sectret.txt", 'r') as f:
    openai.api_key = f.read()

# async 
def process_file(job_description, url):
    """The file being processed should be a text file with the job description."""

    prompt = f"""Return a summarized version as a JSON FILE of following template from the Job Description that will follow (retain all relevant information & ):
JOB_DETAILS = {{
job_title: <<job_title>>,
experience_required: <<experience_required>>,
education_required: <<education_required>>,
certifications_required: <<certifications_required - just list them>>,
salary: <<salary>>,
comapny: <<comapny>>,
date_posted: <<date_posted YYYY-MM-DD for reference, today's date is {date.today()} >>,
location: <<location>>,
job_type: <<job_type>>,
is_remote: <<BOOLEAN - try and determine whether the job is actually remote - many companies are labeled "remote" but the ad says otherwise.>>,
languages: <<languages - English, French, or Bilingual Requirements>>,
}}

<<JOB DESCRIPTION>>
"""
    user_message = f"{prompt} {job_description}".replace('\n', '')
    logging.info(f"Calling OpenAI API: {user_message}")
    response = openai.ChatCompletion.create(
        model="gpt-4",
        temperature=0.4,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant that helps people find jobs. "
                    "You are given a job description and you need to extract the "
                    "provided information from the job description. You are given "
                    "a template to fill in. Your response needs to be a python dictionary, "
                    "matching the template exactly. The template is a dictionary,"
                    "with both value and key being strings."
                ),
            },
            {"role": "user", "content": user_message},
        ],
    )
    formatted_response = response.choices[0].message.content
    print(formatted_response)
    # Insert to database
    def insert_to_db(df, db_name, table_name):
        import sqlite3
        db_conn = sqlite3.connect(f'{db_name}.db')
        df.to_sql(table_name, db_conn, if_exists='append', index=False)
        db_conn.close()
        
    
    


async def main():
    import pandas as pd
    import sqlite3
    sql = """select distinct * from step_2__text_from_job_description_page 
    where page_text not like '%Checking if the site connection is secure%'
    limit 50
    """
    db_name = 'step_2__text_from_job_description_page'
    db_conn = sqlite3.connect(f'{db_name}.db')
    job_descriptions = pd.read_sql(sql, con=db_conn).reset_index(drop=True)
    
    rate_limit = 25
    time_interval = 60
    tasks = []
    for index, job_description in enumerate(job_descriptions['page_text']):
        tasks.append(asyncio.to_thread(process_file, job_description.replace('\n', ' '), job_descriptions['urls']))
        if len(tasks) >= rate_limit:
            await asyncio.gather(*tasks)
            tasks.clear()
            time.sleep(time_interval)
    if tasks:
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())



    """
    
    TODO:
    -- openai.error.InvalidRequestError: This model's maximum context length is 4097 tokens. However, your messages resulted in 4210 tokens. Please reduce the length of the messages.
    -- also theres no way to get the file name in the prompt output? or some way to track it.
    """