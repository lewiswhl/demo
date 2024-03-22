from bs4 import BeautifulSoup
from selenium import webdriver
import time
import os
import psycopg2
import json
import html2text
pj_path = r'C:\github\miscellaneous\python\project\job_seeking'
count = [0]
conn = psycopg2.connect(dbname='postgres', user='', password='', host='localhost', port='5432')
cur = conn.cursor()

def html_writer(bs_obj, lt_count=count):
    if not isinstance(bs_obj,list):
        bs_obj = [bs_obj]
    for i in bs_obj:
        count_val = lt_count[0]
        print(count_val)
        with open(os.path.join(pj_path,f'test_{count_val}.txt'), 'w',encoding='utf8') as f:
            f.write(str(i))
        lt_count[0] = count_val + 1

def finder(soup, element, substring):
    return soup.find_all(lambda tag: tag.name == element and tag.get('class') and substring in ' '.join(tag['class']))

def get_value(bs4_object, substring=''):
    try:
        value = bs4_object.find(class_=lambda c: c and substring in c).text.strip()
        return value
    except AttributeError:
        return '*NOT SPECIFIED*'
def get_url(bs4_object, substring=''):
    try:
        value = bs4_object.find(class_=lambda c: c and substring in c)['href']
        return value
    except:
        return '*NOT SPECIFIED*'

l=list()
o={}

target_url = "https://www.glassdoor.ca/Job/toronto-on-canada-tableau-jobs-SRCH_IL.0,17_IC2281069_KO18,25.htm?sortBy=date_desc"
options = webdriver.ChromeOptions()
options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) ''Chrome/94.0.4606.81 Safari/537.36')

driver = webdriver.Chrome(options=options)
driver.get(target_url)
resp = driver.page_source
driver.close()

soup=BeautifulSoup(resp,'html.parser')
allJobsContainer = finder(soup, 'div', 'JobCard_jobCardContent')

sql_table_create = """create table if not exists career.glassdoor_job (
job_listing_id varchar primary key,
company_name varchar not null,
job_title varchar not null,
salary varchar not null,
location varchar not null,
job_listing_url varchar not null,
is_parsed boolean default false
);"""
cur.execute(sql_table_create)
conn.commit()

for job in allJobsContainer:
    dt = {
    'company_name':get_value(job, 'EmployerProfile_compactEmployerName'),
    'job_title':get_value(job, 'JobCard_jobTitle'),
    'salary':get_value(job, 'JobCard_salaryEstimate'),
    'location':get_value(job, 'JobCard_location'),
    'job_listing_url':get_url(job, 'JobCard_jobTitle'),
    'job_listing_id':get_url(job, 'JobCard_jobTitle').split('?jl=')[1]
    }
    sql = """insert into career.glassdoor_job (job_listing_id,company_name,job_title,salary, location, job_listing_url) 
    values (%(job_listing_id)s,%(company_name)s,%(job_title)s,%(salary)s,%(location)s,%(job_listing_url)s) on conflict do nothing;"""
    cur.execute(sql,dt)
    conn.commit()

sql_table_create = """CREATE TABLE IF NOT EXISTS career.glassdoor_detail (
    job_listing_id varchar PRIMARY KEY,
    title VARCHAR,
    date_posted DATE,
    employment_type TEXT[],
    valid_through DATE,
    organization_name VARCHAR,
    organization_logo VARCHAR,
    organization_url VARCHAR,
    job_location_locality VARCHAR,
    job_location_region VARCHAR,
    job_location_country VARCHAR,
    job_location_latitude FLOAT,
    job_location_longitude FLOAT,
    description TEXT,
    salary_currency VARCHAR,
    salary_min_value INTEGER,
    salary_max_value INTEGER,
    salary_unit_text VARCHAR,
    education_requirements TEXT,
    experience_requirements TEXT,
    experience_months INTEGER,
    experience_description TEXT,
    industry VARCHAR,
    direct_apply BOOLEAN
);"""
cur.execute(sql_table_create)
conn.commit()

def get_value(key_chain, data):
    try:
        value = data
        for key in key_chain:
            value = value.get(key)
        return value
    except (AttributeError, KeyError):
        return None

sql = """select * from career.glassdoor_job where not is_parsed"""
cur.execute(sql)
data = cur.fetchall()
h = html2text.HTML2Text()
for index, row in enumerate(data[:]):
    url = row[-2]
    id = row[0]
    print(url)
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    resp = driver.page_source
    driver.close()
    parsed = BeautifulSoup(resp,'html.parser')
    with open(os.path.join(pj_path,'indi.html'),'w', encoding='utf-8') as f:
        f.write(str(parsed))
    script_tag = parsed.find('script', type='application/ld+json')
    try:
        text = script_tag.text.strip()
    except:
        continue
    data = json.loads(text)
    title = get_value(['title'], data)
    date_posted = get_value(['datePosted'], data)
    employment_type = get_value(['employmentType'], data)
    valid_through = get_value(['validThrough'], data)
    organization = get_value(['hiringOrganization'], data)
    organization_name = get_value(['name'], organization)
    organization_logo = get_value(['logo'], organization)
    organization_url = get_value(['sameAs'], organization)
    job_location = get_value(['jobLocation'], data)
    job_location_locality = get_value(['address', 'addressLocality'], job_location)
    job_location_region = get_value(['address', 'addressRegion'], job_location)
    job_location_country = get_value(['address', 'addressCountry', 'name'], job_location)
    job_location_latitude = get_value(['geo', 'latitude'], job_location)
    job_location_longitude = get_value(['geo', 'longitude'], job_location)
    description = get_value(['description'], data)
    salary_currency = get_value(['salaryCurrency'], data)
    salary_min_value = get_value(['estimatedSalary', 'value', 'minValue'], data)
    salary_max_value = get_value(['estimatedSalary', 'value', 'maxValue'], data)
    salary_unit_text = get_value(['estimatedSalary', 'value', 'unitText'], data)
    education_requirements = get_value(['educationRequirements', 'credentialCategory'], data)
    experience_requirements = get_value(['experienceRequirements', 'description'], data)
    experience_months = get_value(['experienceRequirements', 'monthsOfExperience'], data)
    experience_description = get_value(['experienceRequirements', 'description'], data)
    industry = get_value(['industry'], data)
    direct_apply = get_value(['directApply'], data)
    print(index, 'trying', organization_name, title)
    # Insert the values into the table
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO career.glassdoor_detail (
            job_listing_id,
             title,
             date_posted,
             employment_type,
             valid_through,

            organization_name,
             organization_logo,
            organization_url,
             job_location_locality,
             job_location_region,

             job_location_country,
            job_location_latitude,
             job_location_longitude,
             description,
             salary_currency,

            salary_min_value,
             salary_max_value,
             salary_unit_text,
             education_requirements,
             experience_requirements,

             experience_months,
             experience_description,
             industry,
             direct_apply
        )
        VALUES (
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s
        )
        on conflict do nothing;
    """
    lines = h.handle(description).splitlines()
    non_empty_lines = [line for line in lines if line.strip() != ""]
    formatted_description = "\n".join(non_empty_lines)

    cursor.execute(insert_query, (
        id, title, date_posted, employment_type, valid_through, organization_name, organization_logo,
        organization_url, job_location_locality, job_location_region, job_location_country,
        job_location_latitude, job_location_longitude, formatted_description, salary_currency,
        salary_min_value, salary_max_value, salary_unit_text, education_requirements,
        experience_requirements, experience_months, experience_description, industry, direct_apply
    ))
    conn.commit()

    sql_update_main = f"""update career.glassdoor_job set is_parsed = True where job_listing_id = '{id}'"""
    cur.execute(sql_update_main)
    conn.commit()
    time.sleep(10)

cursor.close()
conn.close()