import pandas as pd
from unidecode import unidecode
import json
import pysolr
from dateutil.relativedelta import relativedelta
import numpy as np
import subprocess


def f(dob):
    r = relativedelta(pd.to_datetime("now"), dob)

    return r.years


def format_df():
    df = pd.read_csv("aluno.csv")
    df.columns = [col.lower() for col in df]
    df.columns = [col.replace(" ", "_") for col in df]
    df.columns = [unidecode(col) for col in df]
    df["data_de_nascimento"] = pd.to_datetime(df["data_de_nascimento"])
    df["warnings"] = df["data_de_nascimento"].apply(f)
    np.where(df["idade"] == df["warnings"], "", "DOB and age dont match")
    df.fillna("")
    json_object = json.loads(df.to_json(orient="records"))
    return json_object


def main():
    json_to_upload = format_df()
    core_name = "aluno2"
    docker_run = f"docker exec -it solr_instance solr create_core -c {core_name}"
    subprocess.call(docker_run, shell=True)

    solr = pysolr.Solr(f"http://localhost:8983/solr/{core_name}")
    solr.add(json_to_upload)
    solr.commit()


if __name__ == "__main__":
    main()
