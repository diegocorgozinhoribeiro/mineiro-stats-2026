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
# Ajuste de importação para versões mais recentes do webdriver_manager
try:
    from webdriver_manager.core.os_manager import ChromeType
except ImportError:
    # Fallback genérico ou para versões onde utils existia (raro hoje em dia, mas mantém compatibilidade)
    ChromeType = None

def executar_atualizacao():
    print("--- INICIANDO PROCESSO DE SCRAPING AUTOMÁTICO ---")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    # Importante: Simular um navegador real para evitar bloqueios
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

    # Define explicitamente o caminho do binário (específico para o ambiente do seu servidor)
    options.binary_location = "/usr/bin/chromium"

    driver = None
    try:
        print("Configurando driver...")
        # Tenta usar ChromeType se a importação funcionou, senão vai no padrão
        if ChromeType and hasattr(ChromeType, 'CHROMIUM'):
            manager = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM)
        else:
            manager = ChromeDriverManager() # Tenta detectar automático ou padrão

        service = Service(manager.install())
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f"Erro na configuração automática: {e}")
        try:
            print("Tentando inicialização direta com Selenium Manager...")
            driver = webdriver.Chrome(options=options)
        except Exception as e2:
            print(f"Falha crítica ao iniciar driver: {e2}")
            return

    url = "https://optaplayerstats.statsperform.com/en_GB/soccer/mineiro-1-2026/5sgngcwblcoi5lqglrkr3q42c/opta-player-stats"

    try:
        print("Acessando URL...")
        driver.get(url)

        # Espera explícita pelo carregamento da tabela (até 20 segundos)
        print("Aguardando carregamento da tabela...")
        wait = WebDriverWait(driver, 20)
        tabela = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "Opta-Crested")))

        # Pequena pausa extra para garantir que o conteúdo dentro da tabela renderizou
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

                    # Tenta extrair o placar com tratamento de erro caso o jogo não tenha ocorrido
                    try:
                        gols_casa = int(segmento.find_element(By.CSS_SELECTOR, "td.Opta-Home.Opta-Score span").text)
                        gols_fora = int(segmento.find_element(By.CSS_SELECTOR, "td.Opta-Away.Opta-Score span").text)
                    except:
                        # Se der erro ao converter int, provavelmente o jogo não começou ou é '-'
                        continue

                    row = {
                        "match_id": match_id,
                        "time_casa": nome_casa,
                        "gols_casa": gols_casa,
                        "time_fora": nome_fora,
                        "gols_fora": gols_fora,
                    }
                    dados_lista.append(row)
                except Exception as e:
                    # Logs detalhados apenas se precisar debug
                    # print(f"Pular linha: {e}")
                    continue

        df = pd.DataFrame(dados_lista)

        if not df.empty:
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
            print("Nenhum dado válido encontrado após scraping (DataFrame vazio).")

    except Exception as e:
        print(f"Erro durante a execução do scraping: {e}")
        # Tira um print da tela para debug se der erro (opcional, só salva localmente)
        try:
            driver.save_screenshot("erro_scraping.png")
            print("Screenshot de erro salvo como erro_scraping.png")
        except:
            pass

    finally:
        if driver:
            driver.quit()
        print("--- FIM DO SCRAPING ---")

if __name__ == "__main__":
    executar_atualizacao()
