import time
import os
import pandas as pd
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
# Importando do novo arquivo centralizador
from database import get_sqlalchemy_conn_string

try:
    from webdriver_manager.core.os_manager import ChromeType
except ImportError:
    ChromeType = None

def executar_atualizacao():
    print("--- INICIANDO PROCESSO DE SCRAPING AUTOMÁTICO ---")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

    # Ajuste este caminho se necessário
    if os.path.exists("/usr/bin/chromium"):
        options.binary_location = "/usr/bin/chromium"

    driver = None
    try:
        print("Configurando driver...")
        if ChromeType and hasattr(ChromeType, 'CHROMIUM'):
            manager = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM)
        else:
            manager = ChromeDriverManager()

        service = Service(manager.install())
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f"Erro na configuração automática: {e}")
        try:
            driver = webdriver.Chrome(options=options)
        except Exception as e2:
            print(f"Falha crítica ao iniciar driver: {e2}")
            return

    url = "https://optaplayerstats.statsperform.com/en_GB/soccer/mineiro-1-2026/5sgngcwblcoi5lqglrkr3q42c/opta-player-stats"

    try:
        print("Acessando URL...")
        driver.get(url)

        print("Aguardando carregamento da tabela...")
        wait = WebDriverWait(driver, 25)
        tabela = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "Opta-Crested")))
        time.sleep(3)

        segmentos = tabela.find_elements(By.TAG_NAME, "tbody")
        dados_lista = []
        print(f"Encontrados {len(segmentos)} segmentos de tabela. Processando...")

        for segmento in segmentos:
            classes = segmento.get_attribute("class")

            if "Opta-fixture" in classes:
                try:
                    nome_casa = segmento.find_element(By.CLASS_NAME, "Opta-Home.Opta-TeamName").text
                    nome_fora = segmento.find_element(By.CLASS_NAME, "Opta-Away.Opta-TeamName").text
                    match_id = nome_casa + nome_fora

                    try:
                        gols_casa = int(segmento.find_element(By.CSS_SELECTOR, "td.Opta-Home.Opta-Score span").text)
                        gols_fora = int(segmento.find_element(By.CSS_SELECTOR, "td.Opta-Away.Opta-Score span").text)
                    except:
                        continue

                    row = {
                        "match_id": match_id,
                        "time_casa": nome_casa,
                        "gols_casa": gols_casa,
                        "time_fora": nome_fora,
                        "gols_fora": gols_fora,
                    }
                    dados_lista.append(row)
                except:
                    continue

        df = pd.DataFrame(dados_lista)

        if not df.empty:
            # USANDO A FUNÇÃO CENTRALIZADA
            connection_string = get_sqlalchemy_conn_string()
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

            print(f"Sucesso! {len(df)} jogos processados.")
        else:
            print("Nenhum dado válido encontrado.")

    except Exception as e:
        print(f"Erro durante scraping: {e}")
    finally:
        if driver: driver.quit()
        print("--- FIM DO SCRAPING ---")

if __name__ == "__main__":
    executar_atualizacao()
