import json
import pysolr
import subprocess
import requests
import pandas as pd
from unidecode import unidecode
from dateutil.relativedelta import relativedelta


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

    for i in df.index:
        if df.at[i, "idade"] == df.at[i, "warnings"]:
            df.at[i, "warnings"] = ""
        else:
            df.at[i, "warnings"] = "Data de nascimento e idade são incopatíveis"

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
        {"name": "endereco", "type": "textField", "stored": "true"},
        {"name": "nome_pai", "type": "textField", "stored": "true"},
        {"name": "nome_mae", "type": "textField", "stored": "true"},
        {"name": "nota_media", "type": "pfloat", "stored": "true", "required": "true"},
        {"name": "data_de_nascimento", "type": "pdate", "stored": "true"},
        {"name": "warnings", "type": "textField", "stored": "true"},
    ]

    for field in field_list:
        json_data = {"add-field": field}
        requests.post(
            "http://localhost:8983/solr/teste/schema", headers=headers, json=json_data
        )


def main():
    json_to_upload = get_df_json()
    core_name = "aluno"
    config_core(core_name)
    solr = pysolr.Solr(f"http://localhost:8983/solr/{core_name}")
    solr.add(json_to_upload)
    solr.commit()


if __name__ == "__main__":
    main()
