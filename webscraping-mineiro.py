import time
from operator import concat

import pandas as pd
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Substitua pela URL onde você encontrou essa tabela
url = "https://optaplayerstats.statsperform.com/en_GB/soccer/mineiro-1-2026/5sgngcwblcoi5lqglrkr3q42c/opta-player-stats"

try:
    driver.get(url)
    time.sleep(5)

    tabela = driver.find_element(By.CLASS_NAME, "Opta-Crested")
    segmentos = tabela.find_elements(By.TAG_NAME, "tbody")

    data_atual = ""
    dados_lista = []

    for segmento in segmentos:
        classes = segmento.get_attribute("class")

        if "Opta-fixture" not in classes:
            try:
                data_ext = segmento.find_element(By.TAG_NAME, "h3").text
                if data_ext: data_atual = data_ext
            except: continue
        else:
            try:
                # Extração dos campos
                row = {
                    "match_id": concat(segmento.find_element(By.CLASS_NAME, "Opta-Home.Opta-TeamName").text, segmento.find_element(By.CLASS_NAME, "Opta-Away.Opta-TeamName").text),
                    "time_casa": segmento.find_element(By.CLASS_NAME, "Opta-Home.Opta-TeamName").text,
                    "gols_casa": int(segmento.find_element(By.CSS_SELECTOR, "td.Opta-Home.Opta-Score span").text),
                    "time_fora": segmento.find_element(By.CLASS_NAME, "Opta-Away.Opta-TeamName").text,
                    "gols_fora": int(segmento.find_element(By.CSS_SELECTOR, "td.Opta-Away.Opta-Score span").text),
                }
                dados_lista.append(row)
            except: continue

    # 2. Transformação em DataFrame
    df = pd.DataFrame(dados_lista)
    print(df)

    engine = create_engine("mysql+pymysql://root:1234@localhost:3306/mineiro")

    # 1. Enviar para uma tabela temporária (staged)
    df.to_sql('staged_resultados', con=engine, if_exists='replace', index=False)

    # 2. Executar o INSERT IGNORE ou ON DUPLICATE KEY UPDATE
    # Isso move os dados da 'staged' para a oficial 'resultados_jogos'
    # A query agora mapeia exatamente qual coluna da staging vai para qual coluna da oficial
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
        # Remove a tabela temporária após o processo
        conn.execute(text("DROP TABLE staged_resultados;"))

    print(f"Processo concluído. {len(df)} jogos processados (duplicatas foram atualizadas).")

finally:
    driver.quit()