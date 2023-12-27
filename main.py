import pandas as pd
from unidecode  import unidecode

def main():
    df = pd.read_csv('aluno.csv')
    df.columns=[col.lower() for col in df]
    df.columns=[col.replace(" ", "_") for col in df]
    df.columns=[unidecode(col) for col in df]
    # breakpoint()
    print('oi')
    
main()