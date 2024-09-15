from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import psycopg2
from bs4 import BeautifulSoup
import pytz

# Configurações de conexão ao banco de dados PostgreSQL
db_config = {
    'host': '192.168.0.254',
    'port': 5432,
    'database': 'DataLake_OfertasML',
    'user': 'postgres',
    'password': '1234'
}

# Lista de URLs das páginas a serem coletadas
urls = [
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=2",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=3",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=4",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=5"
]

# Função para fazer o scraping
def scrape_data():
    ofertas = []
    def identificar_marca(nome_produto):
        marcas = ['Samsung', 'Motorola', 'Apple', 'Xiaomi', 'Realme', 'Multilaser']
        for marca in marcas:
            if marca.lower() in nome_produto.lower():
                return marca
        return 'Outros'

    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        produtos = soup.find_all('li', class_='promotion-item')

        for produto in produtos:
            titulo = produto.find('p', class_='promotion-item__title')
            nome_produto = titulo.text if titulo else 'N/A'
            marca = identificar_marca(nome_produto)
            preco_atual = produto.find('span', class_='andes-money-amount__fraction')
            preco_atual_texto = preco_atual.text if preco_atual else 'N/A'
            preco_antigo = produto.find_all('span', class_='andes-money-amount__fraction')
            preco_antigo_texto = preco_antigo[1].text if len(preco_antigo) > 1 else 'N/A'
            loja_vendedora = produto.find('span', class_='promotion-item__seller')
            loja_vendedora_texto = loja_vendedora.text if loja_vendedora else 'N/A'
            oferta_do_dia = produto.find('span', class_='promotion-item__today-offer-text')
            oferta_do_dia_texto = oferta_do_dia.text if oferta_do_dia else 'SEM OFERTA DO DIA'
            frete_full = produto.find('svg', class_='full-icon')
            frete_full_texto = 'FRETE FULL' if frete_full else 'SEM FRETE FULL'
            porcentagem_desconto = produto.find('span', class_='promotion-item__discount-text')
            porcentagem_desconto_texto = porcentagem_desconto.text if porcentagem_desconto else 'SEM DESCONTO'
            imagem_produto = produto.find('img', class_='promotion-item__img')
            imagem_produto_url = imagem_produto['src'] if imagem_produto else 'N/A'

            ofertas.append({
                'nome_produto': nome_produto,
                'marca': marca,
                'preco_atual': preco_atual_texto,
                'preco_antigo': preco_antigo_texto,
                'loja_vendedora': loja_vendedora_texto,
                'oferta_do_dia': oferta_do_dia_texto,
                'frete_full': frete_full_texto,
                'porcentagem_desconto': porcentagem_desconto_texto,
                'imagem_produto_url': imagem_produto_url
            })
    return ofertas

# Função para transformar os dados em tabela
def transformar_dados(ti):
    ofertas = ti.xcom_pull(task_ids='scrape_data')
    df_ofertas = pd.DataFrame(ofertas)
    ti.xcom_push(key='df_ofertas', value=df_ofertas)

# Função para inserir os dados no PostgreSQL
def inserir_dados(ti):
    df_ofertas = ti.xcom_pull(key='df_ofertas', task_ids='transformar_dados')
    conn = psycopg2.connect(**db_config)
    
    def criar_tabela_se_nao_existir(conn):
        query = """
        CREATE TABLE IF NOT EXISTS DL_OfertasCelularesML (
            id SERIAL PRIMARY KEY,
            nome_produto VARCHAR(255),
            marca VARCHAR(50),
            preco_atual VARCHAR(50),
            preco_antigo VARCHAR(50),
            loja_vendedora VARCHAR(255),
            oferta_do_dia VARCHAR(255),
            frete_full VARCHAR(50),
            porcentagem_desconto VARCHAR(50),
            imagem_produto_url TEXT,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

    def esvaziar_tabela(conn):
        query = "TRUNCATE TABLE DL_OfertasCelularesML;"
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

    def inserir_dados(conn, df):
        for _, row in df.iterrows():
            query = """
            INSERT INTO DL_OfertasCelularesML (
                nome_produto, marca, preco_atual, preco_antigo, loja_vendedora,
                oferta_do_dia, frete_full, porcentagem_desconto, imagem_produto_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            data = (
                row['nome_produto'], row['marca'], row['preco_atual'], row['preco_antigo'],
                row['loja_vendedora'], row['oferta_do_dia'], row['frete_full'],
                row['porcentagem_desconto'], row['imagem_produto_url']
            )
            with conn.cursor() as cur:
                cur.execute(query, data)
        conn.commit()

    try:
        criar_tabela_se_nao_existir(conn)
        esvaziar_tabela(conn)
        inserir_dados(conn, df_ofertas)
        print("Dados inseridos com sucesso!")
    finally:
        conn.close()

# Configurações da DAG
with DAG(
    'ELT_OfertasMercadoLivre',
    description='ELT das ofertas do mercado livre, agendado todo dia as 8:00 horas',
    schedule_interval='0 8 * * *',
    start_date=datetime(2023, 9, 8, tzinfo=pytz.timezone('America/Cuiaba')),
    catchup=False
) as dag:

    task_scrape_data = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_data
    )

    task_transformar_dados = PythonOperator(
        task_id='transformar_dados',
        python_callable=transformar_dados
    )

    task_inserir_dados = PythonOperator(
        task_id='inserir_dados',
        python_callable=inserir_dados
    )

    task_scrape_data >> task_transformar_dados >> task_inserir_dados
