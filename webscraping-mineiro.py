import time
import os
import pandas as pd
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

def executar_atualizacao():
    print("--- INICIANDO PROCESSO DE SCRAPING AUTOMÁTICO ---")

    # --- CONFIGURAÇÃO PARA RODAR EM SERVIDOR (HEADLESS) ---
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    # Tenta instalar o driver automaticamente
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    url = "https://optaplayerstats.statsperform.com/en_GB/soccer/mineiro-1-2026/5sgngcwblcoi5lqglrkr3q42c/opta-player-stats"

    try:
        print("Acessando URL...")
        driver.get(url)
        time.sleep(5)  # Aguarda carregamento do JS

        tabela = driver.find_element(By.CLASS_NAME, "Opta-Crested")
        segmentos = tabela.find_elements(By.TAG_NAME, "tbody")

        dados_lista = []

        for segmento in segmentos:
            classes = segmento.get_attribute("class")

            if "Opta-fixture" in classes:
                try:
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
                    continue

        df = pd.DataFrame(dados_lista)

        if not df.empty:
            # --- CONFIGURAÇÃO DE BANCO VIA VARIÁVEIS DE AMBIENTE ---
            db_user = os.environ.get('DB_USER', 'root')
            db_pass = os.environ.get('DB_PASS', '1234')
            db_host = os.environ.get('DB_HOST', 'localhost')
            db_port = os.environ.get('DB_PORT', '3306')
            db_name = os.environ.get('DB_NAME', 'mineiro')

            connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(connection_string)

            print("Atualizando banco de dados...")

            df.to_sql('staged_resultados', con=engine, if_exists='replace', index=False)

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

            print(f"Sucesso! {len(df)} jogos processados e atualizados.")
        else:
            print("Nenhum dado encontrado no scraping.")

    except Exception as e:
        print(f"Erro no scraping: {e}")

    finally:
        driver.quit()
        print("--- FIM DO SCRAPING ---")

# Permite rodar o arquivo diretamente também: python webscraping-mineiro.py
if __name__ == "__main__":
    executar_atualizacao()
