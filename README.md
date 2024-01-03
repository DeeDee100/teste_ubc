
# Desafio UBC

Este projeto consiste em realizar a leitura de um arquivo csv e enviar as informações para uma instancia do Apache Solr. 
As instruções completas podem ser encotradas [**neste link**](https://github.com/joaomarcelo81/DesafioUBC/)


## Como rodar o projeto
 É necesário ter Python e o Docker instalado na máquina caso não tenha é possível obté-los na página de [download do Python](https://www.python.org/downloads/) e na página de [instruçoes do Docker](https://docs.docker.com/get-docker/) 

1. Primeiro é necessário criar uma instancia do Solr no Docker:
```bash
    docker run -d -p 8983:8983 --name solr_instance -t solr
```
2. Após a criação da instância é preciso instalar as dependências do script python utilizando o comando:

```bash
    pip install -r requirements.txt
```
3.  Por último basta executar usando o comando:
```bash
    python main.py
```

## Sobre o Projeto


O objetivo deste projeto é reeceber dados a partir de um csv tratar as informações e upá-las no Solr. O script possui capacidade de criar um core e configurar o schema para a tabela de alunos. Também possui [uma branch](https://github.com/DeeDee100/teste_ubc/tree/pega-tds-csv) capaz de pegar cada arquivo csv da pasta e enviarr as informações para um mesmo core.

Tratamentos atualmente implementados:
* Formatação do nome das colunas.
* Cria uma coluna Warning e quando há dados vazios os preenche com '0' e adiciona essa informação na coluna bem como cria o log no terminal dessa informação.
* Quando a idade e a data de nascimento não coincidem cria uma nota na coluna warning com essa informação e um log no terminal.

---
<img src="https://img.shields.io/badge/Made%20with-python-1f425f.svg" />
