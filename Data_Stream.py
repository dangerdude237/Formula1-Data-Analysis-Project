import requests
from bs4 import BeautifulSoup
import pandas as pd 
import argparse
from kafka import KafkaProducer
import json
# from airflow import DAG
# from airflow 

def preprocess(text):
  cleaned_text = text.replace("\\n", "")
  cleaned_text = cleaned_text.replace(" ", "")
  list_1 = cleaned_text.split("\n")
  return " ".join(list_1[:2])

def scrape_race_data(race_url):
    response = requests.get(race_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    year,race = race_url.split("/")[5],race_url.split("/")[8]
    race_table = soup.find('table', class_='resultsarchive-table')
    if not race_table:
        return None
    headers = [header.text.strip() for header in race_table.find_all('th')]
    rows = []
    for row in race_table.find_all('tr')[1:]:
        rows.append([col.text.strip() for col in row.find_all('td')])

    df_1 = pd.DataFrame(rows, columns=headers)
    df_1 = df_1.iloc[:,1:-1]
    df_1["Year"] = year
    df_1["Grand Prix"] = race

    return df_1

def scrape_quali_data(race_url):
  race_url = race_url.replace("race-result.html","starting-grid.html")
  response = requests.get(race_url)
  if response.status_code != 200:
    return pd.DataFrame()
  soup = BeautifulSoup(response.content, 'html.parser')
  year,race = race_url.split("/")[5],race_url.split("/")[8]
  race_table = soup.find('table', class_='resultsarchive-table')
  if not race_table:
      return None
  headers = [header.text.strip() for header in race_table.find_all('th')]
  rows = []

  for row in race_table.find_all('tr')[1:]:
      rows.append([col.text.strip() for col in row.find_all('td')])

  df_1 = pd.DataFrame(rows, columns=headers)
  df_1 = df_1.iloc[:,1:-1]
  df_1["Year"] = year
  df_1["Grand Prix"] = race

  

  return df_1

def get_data(year,grand_prix):
    url = f'https://www.formula1.com/en/results.html/{year}/races.html'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    results_table = soup.find('table', class_='resultsarchive-table')
    if not results_table:
        return None
    
    for row in results_table.find_all('tr')[1:]:
        cols = [col.text.strip() for col in row.find_all('td')]
        race_name = cols[1].lower().replace(" ", "-")
        if(race_name == grand_prix):
            race_link = row.find('a', href=True)
            race_url = f"https://www.formula1.com{race_link['href']}" if race_link else 'N/A'
            df_1,df_2 = scrape_race_data(race_url),scrape_quali_data(race_url)
        
        df_1["Driver"] = df_1["Driver"].apply(preprocess)
        df_2["Driver"] = df_2["Driver"].apply(preprocess)
        return  df_1.to_dict("records"),df_2.to_dict("records")

def send_data_1(df_1):
    producer = KafkaProducer(bootstrap_servers = ["localhost:9092"],max_block_ms = 5000)
    for row in df_1:
        producer.send("users_created",json.dumps(row).encode("utf-8"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Formual 1 Data Pipeline')
    parser.add_argument('-year', type=int, required=True, help='Year')
    parser.add_argument('-Grand_Prix', type=str, required=True, help='Grand Prix Name')
    args = parser.parse_args()
    df_1,df_2 = get_data(args.year,args.Grand_Prix)
    send_data_1(df_1)

