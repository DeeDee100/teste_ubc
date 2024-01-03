import os
import requests
import pandas as pd
from unidecode import unidecode
import json
import pysolr
from dateutil.relativedelta import relativedelta
import subprocess

def get_age(dob):
    r = relativedelta(pd.to_datetime("now"), dob)

    return r.years



def get_df_json(csv_name):
    df = pd.read_csv(csv_name)
    df.columns = [col.lower() for col in df]
    df.columns = [col.replace(" ", "_") for col in df]
    df.columns = [unidecode(col) for col in df]
    df["timestamp_dob"] = pd.to_datetime(df["data_de_nascimento"])
    df["warnings"] = df["timestamp_dob"].apply(get_age)
    df.drop("timestamp_dob", axis=1, inplace=True)
    nan_rows = df[df.isnull().any(axis=1)].index.to_list()
    df.fillna(0, inplace=True)
    for i in df.index:
        if df.at[i, "idade"] == df.at[i, "warnings"]:
            df.at[i, "warnings"] = ""
        else:
            df.at[i, "warnings"] = "Data de nascimento e idade são incopatíveis;"
            print(f"Data de nascimento e idade nao coincidem, warning criado no index {i}")
        if i in nan_rows:
            df.at[i, "warnings"] = df.at[i, "warnings"] + "Nan encontrado, preenchido com 0;"
            print(f"NaN encontrado, prenchido com 0 index {i}")
    
    json_object = json.loads(df.to_json(orient="records"))
    return json_object

def config_core(core_name):
    docker_run = f"docker exec -it solr_instance solr create_core -c {core_name}"
    subprocess.call(docker_run, shell=True)
    headers = {"Content-type": "application/json"}
    field_list = [
        {"name": "nome", "type": "text_general", "stored": "true", "required": "true"},
        {"name": "idade", "type": "pint", "stored": "true", "required": "true"},
        {"name": "serie", "type": "pint", "stored": "true", "required": "true"},
        {"name": "endereco", "type": "text_general", "stored": "true"},
        {"name": "nome_pai", "type": "text_general", "stored": "true"},
        {"name": "nome_mae", "type": "text_general", "stored": "true"},
        {"name": "nota_media", "type": "pfloat", "stored": "true", "required": "true"},
        {"name": "data_de_nascimento", "type": "pdate", "stored": "true"},
        {"name": "warnings", "type": "text_general", "stored": "true"}
    ]

    for field in field_list:
        json_data = {"add-field": field}
        response = requests.post(
            f"http://localhost:8983/solr/{core_name}/schema", headers=headers, json=json_data
        )
        if response.status_code != 200:
            print(f'Campo não criado - log, erro: \n{response.content}')
    print(f"Core {core_name} criado com sucesso.")

def get_all_csv():
    path = "."
    names_list = []

    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            names_list.append(filename)

    return names_list

def main():
    csv_list = get_all_csv()
    for csv in csv_list:
        core_name = "aluno"
        solr = pysolr.Solr(f"http://localhost:8983/solr/{core_name}")
        try:
            solr.ping()
            print('Core encontrado, pulando criação\n')
        except pysolr.SolrError:
            print('Criando core...')
            config_core(core_name)
        json_to_upload = get_df_json(csv)
        solr.add(json_to_upload)
        solr.commit()
        print("Upload realizado com sucesso.")



if __name__ == "__main__":
    main()
