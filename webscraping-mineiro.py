import time
import os
from operator import concat
import pandas as pd
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURAÇÃO PARA RODAR EM SERVIDOR (HEADLESS) ---
options = Options()
options.add_argument("--headless") # Essencial: Roda sem abrir janela
options.add_argument("--no-sandbox") # Essencial para Linux/Docker
options.add_argument("--disable-dev-shm-usage") # Evita crash de memória em containers
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")

# Tenta instalar o driver automaticamente
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

url = "https://optaplayerstats.statsperform.com/en_GB/soccer/mineiro-1-2026/5sgngcwblcoi5lqglrkr3q42c/opta-player-stats"

try:
    print("Iniciando scraping...")
    driver.get(url)
    time.sleep(5) # Aguarda carregamento do JS

    tabela = driver.find_element(By.CLASS_NAME, "Opta-Crested")
    segmentos = tabela.find_elements(By.TAG_NAME, "tbody")

    dados_lista = []

    for segmento in segmentos:
        classes = segmento.get_attribute("class")

        if "Opta-fixture" in classes:
            try:
                # Extração dos campos
                nome_casa = segmento.find_element(By.CLASS_NAME, "Opta-Home.Opta-TeamName").text
                nome_fora = segmento.find_element(By.CLASS_NAME, "Opta-Away.Opta-TeamName").text

                # Match ID consistente
                match_id = nome_casa + nome_fora

                row = {
                    "match_id": match_id,
                    "time_casa": nome_casa,
                    "gols_casa": int(segmento.find_element(By.CSS_SELECTOR, "td.Opta-Home.Opta-Score span").text),
                    "time_fora": nome_fora,
                    "gols_fora": int(segmento.find_element(By.CSS_SELECTOR, "td.Opta-Away.Opta-Score span").text),
                }
                dados_lista.append(row)
            except Exception as e:
                # print(f"Erro ao ler linha: {e}")
                continue

    # Transforma em DataFrame
    df = pd.DataFrame(dados_lista)
    print(df)

    if not df.empty:
        # --- CONFIGURAÇÃO DE BANCO VIA VARIÁVEIS DE AMBIENTE ---
        db_user = os.environ.get('DB_USER', 'root')
        db_pass = os.environ.get('DB_PASS', '1234')
        db_host = os.environ.get('DB_HOST', 'localhost') # Na nuvem, isso deve ser o IP do banco
        db_port = os.environ.get('DB_PORT', '3306')
        db_name = os.environ.get('DB_NAME', 'mineiro')

        # Conexão SQLAlchemy
        connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

        print("Conectando ao banco de dados...")

        # 1. Enviar para tabela staged
        df.to_sql('staged_resultados', con=engine, if_exists='replace', index=False)

        # 2. Query de Insert/Update
        query = text("""
                     INSERT INTO resultados_jogos (
                         match_id, time_casa, gols_casa, time_fora, gols_fora
                     )
                     SELECT
                         match_id, time_casa, gols_casa, time_fora, gols_fora
                     FROM staged_resultados
                         ON DUPLICATE KEY UPDATE
                                              gols_casa = VALUES(gols_casa),
                                              gols_fora = VALUES(gols_fora),
                                              data_extracao = CURRENT_TIMESTAMP;
                     """)

        with engine.begin() as conn:
            conn.execute(query)
            conn.execute(text("DROP TABLE staged_resultados;"))

        print(f"Sucesso! {len(df)} jogos processados.")
    else:
        print("Nenhum dado encontrado no scraping.")

except Exception as e:
    print(f"Erro fatal: {e}")

finally:
    driver.quit()
