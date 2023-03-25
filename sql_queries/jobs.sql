select
    job,
    count(job) as job_count
    -- urls,
    -- page_text
from step_2__text_from_job_description_page
where true
and page_text not like '%Checking if the site connection is secure%'
group by 1
order by 2 desc