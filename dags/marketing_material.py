from airflow import DAG
from datetime import datetime
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
import requests
from airflow.providers.postgres.operators.postgres import PostgresOperator
import uuid
from bs4 import BeautifulSoup
import os
from google.cloud import vision
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _extract_data():
    url = "https://www.whosmailingwhat.com/blog/best-direct-mail-marketing-examples/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    image_tags = soup.find_all("img")
    image_tags = [image_tags[27], image_tags[28], image_tags[29]]
    return image_tags

def _recognise_data(ti):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/opt/airflow/config/lab2-407720-06ebaa784705.json'
    image_tags = ti.xcom_pull(f"extract_data")
    client = vision.ImageAnnotatorClient()

    url_regex = re.compile(r'https?://\S+\.(?:com|org|net)\S*|\S+\.(?:com|org|net)')
    recognised_data = []
    for img_tag in image_tags:
        image_url = img_tag["src"]
        image_response = requests.get(image_url)

        if image_response.status_code == 200:
            image = vision.Image(content=image_response.content)

            response = client.text_detection(image=image)
            texts = response.text_annotations if response.text_annotations else []

            recognized_text = texts[0].description
            url = re.search(url_regex, recognized_text)

            if url:
                recognised_data.append({"image_url": image_url, "text_data": recognized_text, "url": url.group(0)})
    return recognised_data

def _check_if_exists(ti):
    recognised_data = ti.xcom_pull(f"recognise_data")

    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("select company_id, url from links;")
    links = cursor.fetchall()
    cursor.execute("select company_id, domain from company_data;")
    existing_company_data = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()

    for image_data in recognised_data:
        company_id = ""
        for id, domain in existing_company_data:
            if image_data['url'] == domain:
                company_id = id
                break
        if not company_id:
            for id, url in links:
                if image_data['url'] == url:
                    company_id = id
                    break
        if company_id:
            image_data['exists'] = True
            image_data['company_id'] = company_id
        else:
            image_data['exists'] = False
    return recognised_data

def _enrich_data(ti):
    recognised_data = ti.xcom_pull(f"check_if_exists")
    url = "https://api.brandfetch.io/v2/brands/"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {Variable.get('BRAND_API_KEY')}"
    }
    companies_data = []
    for image_data in recognised_data:
        if image_data['exists']:
            company_data = \
            {
                "company_id": image_data['company_id'],
                'offer': {
                    'image': image_data['image_url'],
                    'image_text': image_data['text_data']
                },
                'exists': True
            }
        else:
            response = requests.get(url+image_data['url'], headers=headers)
            company_details = response.json()
            company_data = \
            {
                'company_id': str(uuid.uuid4()),
                'name': company_details['name'],
                'domain': company_details['domain'],
                'description': company_details['description'],
                'longDescription': company_details['longDescription'],
                'links': company_details['links'],
                'logos': [{'src': logo_format['src'], "format": logo_format['format']} for logo in company_details['logos'] for logo_format in logo['formats']],
                'offer': {
                    'image': image_data['image_url'],
                    'image_text': image_data['text_data']
                },
                'exists': False
            }
        companies_data.append(company_data)
    
    return companies_data

def _store_data(ti):
    companies_data = ti.xcom_pull(f"enrich_data")

    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_conn()

    cursor = conn.cursor()

    for company_data in companies_data:
        if not company_data['exists']:
            cursor.execute("""
                INSERT INTO company_data (company_id, name, domain, description, long_description)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                company_data['company_id'],
                company_data['name'],
                company_data['domain'],
                company_data['description'],
                company_data['longDescription']
            ))

            for link in company_data['links']:
                cursor.execute("""
                    INSERT INTO links (company_id, name, url)
                    VALUES (%s, %s, %s)
                """, (
                    company_data['company_id'],
                    link['name'],
                    link['url']
                ))

            for logo in company_data['logos']:
                cursor.execute("""
                    INSERT INTO logos (company_id, src, format)
                    VALUES (%s, %s, %s)
                """, (
                    company_data['company_id'],
                    logo['src'],
                    logo['format']
                ))

        cursor.execute("""
            INSERT INTO offers (company_id, image, image_text)
            VALUES (%s, %s, %s)
        """, (
            company_data['company_id'],
            company_data['offer']['image'],
            company_data['offer']['image_text']
        ))


    conn.commit()
    cursor.close()
    conn.close()



with DAG(dag_id="marketing", schedule_interval="@daily", start_date=datetime(2023, 12, 12), catchup=True) as dag:
    create_tables = PostgresOperator(
        task_id="create_tables_postgres",
        postgres_conn_id="postgres_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS company_data (
            company_id          text PRIMARY KEY,
            name                text,
            domain              text,
            description         text,
            long_description    text
        );
        CREATE TABLE IF NOT EXISTS links (
            company_id  text,
            name        text,
            url         text
        );
        CREATE TABLE IF NOT EXISTS logos (
            company_id  text,
            src         text,
            format      text
        );
        CREATE TABLE IF NOT EXISTS offers (
            company_id   text,
            image        text,
            image_text   text
        );"""
    )

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data
    )

    recognise_data = PythonOperator(
        task_id="recognise_data",
        python_callable=_recognise_data
    )

    check_if_exists = PythonOperator(
        task_id="check_if_exists",
        python_callable=_check_if_exists
    )

    enrich_data = PythonOperator(
        task_id="enrich_data",
        python_callable=_enrich_data
    )

    store_data = PythonOperator(
        task_id="store_data",
        python_callable=_store_data
    )

    create_tables >> extract_data >> recognise_data >> check_if_exists >> enrich_data >> store_data