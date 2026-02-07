import mysql.connector
from flask import Flask, render_template, request, jsonify
import json
import itertools
import copy
# Adicionado login_required nos imports
from flask_login import LoginManager, current_user, login_required
# Importa o Blueprint e a função load_user do login.py
from login import auth_bp, load_user as auth_load_user
import os
import gunicorn

app = Flask(__name__)
app.secret_key = 'diego2810'
app.register_blueprint(auth_bp)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login' # Se tentar entrar na home sem logar, vai pra cá

# --- CORREÇÃO IMPORTANTE ---
# Usamos o auth_load_user do login.py para validar o usuário corretamente
@login_manager.user_loader
def loader(user_id):
    return auth_load_user(user_id)

# ---------------------------

app.secret_key = os.environ.get('SECRET_KEY', 'diego2810')

# substituir DB_CONFIG fixo por leitura de variáveis de ambiente
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 3306)),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASS', '1234'),
    'database': os.environ.get('DB_NAME', 'mineiro')
}

GRUPOS_FIXOS = {
    'A': ['URT', 'Democrata GV', 'Atlético MG', 'Uberlândia'],
    'B': ['América MG', 'Pouso Alegre', 'Betim', 'Tombense'],
    'C': ['North', 'Cruzeiro', 'Athletic', 'Itabirito']
}

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def carregar_mapa_logos():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT nome_time, logo_url FROM logo")
        res = cursor.fetchall()
    except:
        res = []
    conn.close()
    return {row['nome_time'].strip(): row['logo_url'] for row in res}

def buscar_dados_brutos():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
            SELECT match_id, time_casa, gols_casa, time_fora, gols_fora, rodada
            FROM resultados_jogos
            ORDER BY rodada ASC, match_id ASC
            """
    cursor.execute(query)
    jogos = cursor.fetchall()
    conn.close()
    return jogos

def processar_tabela_base(jogos, simulacoes_usuario=None, mapa_logos=None):
    if mapa_logos is None: mapa_logos = {}
    if simulacoes_usuario is None: simulacoes_usuario = {}

    tabela = {}
    rodadas_dict = {}
    jogos_abertos = []

    for grupo, times in GRUPOS_FIXOS.items():
        for time in times:
            tabela[time] = {
                'nome': time, 'pontos': 0, 'v': 0, 'e': 0, 'd': 0,
                'gp': 0, 'gc': 0, 'sg': 0, 'grupo': grupo,
                'logo': mapa_logos.get(time, '')
            }

    for jogo in jogos:
        mid = str(jogo['match_id'])
        r = jogo['rodada']
        j = jogo.copy()

        tc_raw = j['time_casa'].strip()
        tf_raw = j['time_fora'].strip()

        j['logo_casa'] = mapa_logos.get(tc_raw, '')
        j['logo_fora'] = mapa_logos.get(tf_raw, '')

        gc, gf = j['gols_casa'], j['gols_fora']
        j['simulado'] = False

        if mid in simulacoes_usuario:
            try:
                gc = int(simulacoes_usuario[mid]['gols_casa'])
                gf = int(simulacoes_usuario[mid]['gols_fora'])
                j['gols_casa'] = gc
                j['gols_fora'] = gf
                j['simulado'] = True
            except:
                pass

        if gc is not None and gf is not None:
            if tc_raw in tabela:
                tabela[tc_raw]['gp'] += gc
                tabela[tc_raw]['gc'] += gf
                tabela[tc_raw]['sg'] += (gc - gf)
            if tf_raw in tabela:
                tabela[tf_raw]['gp'] += gf
                tabela[tf_raw]['gc'] += gc
                tabela[tf_raw]['sg'] += (gf - gc)

            if gc > gf:
                if tc_raw in tabela:
                    tabela[tc_raw]['pontos'] += 3
                    tabela[tc_raw]['v'] += 1
                if tf_raw in tabela:
                    tabela[tf_raw]['d'] += 1
            elif gf > gc:
                if tf_raw in tabela:
                    tabela[tf_raw]['pontos'] += 3
                    tabela[tf_raw]['v'] += 1
                if tc_raw in tabela:
                    tabela[tc_raw]['d'] += 1
            else:
                if tc_raw in tabela:
                    tabela[tc_raw]['pontos'] += 1
                    tabela[tc_raw]['e'] += 1
                if tf_raw in tabela:
                    tabela[tf_raw]['pontos'] += 1
                    tabela[tf_raw]['e'] += 1
        else:
            jogos_abertos.append({
                'time_casa': tc_raw,
                'time_fora': tf_raw
            })

        if r not in rodadas_dict: rodadas_dict[r] = []
        rodadas_dict[r].append(j)

    return tabela, rodadas_dict, jogos_abertos

def ordenar_ranking_final(tabela_dict):
    grupos = {g: [] for g in GRUPOS_FIXOS.keys()}
    for _, stats in tabela_dict.items():
        grupos[stats['grupo']].append(stats)

    def chave_ord(x): return (x['pontos'], x['v'], x['sg'], x['gp'])

    for g in grupos: grupos[g].sort(key=chave_ord, reverse=True)

    p1 = sorted([grupos[g][0] for g in grupos if len(grupos[g])>0], key=chave_ord, reverse=True)
    p2 = sorted([grupos[g][1] for g in grupos if len(grupos[g])>1], key=chave_ord, reverse=True)
    p3 = sorted([grupos[g][2] for g in grupos if len(grupos[g])>2], key=chave_ord, reverse=True)
    p4 = sorted([grupos[g][3] for g in grupos if len(grupos[g])>3], key=chave_ord, reverse=True)

    return p1 + p2 + p3 + p4, grupos

def calcular_probabilidade_exata(tabela_base, jogos_abertos):
    qtd_jogos = len(jogos_abertos)
    LIMITE_JOGOS = 13

    if qtd_jogos > LIMITE_JOGOS:
        jogos_para_processar = jogos_abertos[:LIMITE_JOGOS]
    else:
        jogos_para_processar = jogos_abertos

    possibilidades = [(3, 0, 1, 0), (1, 1, 0, 0), (0, 3, 0, 1)]
    todos_cenarios = itertools.product(possibilidades, repeat=len(jogos_para_processar))

    stats_counts = {time: {'semi': 0, 'inconf': 0, 'rebaix': 0} for time in tabela_base}
    total_cenarios = 0

    base_state_template = {}
    for nome, dados in tabela_base.items():
        base_state_template[nome] = {
            'nome': dados['nome'], 'grupo': dados['grupo'],
            'p': dados['pontos'], 'v': dados['v'], 'sg': dados['sg'], 'gp': dados['gp']
        }

    keys_groups = sorted(GRUPOS_FIXOS.keys())
    key_func = lambda x: (x['p'], x['v'], x['sg'], x['gp'])

    for cenario in todos_cenarios:
        total_cenarios += 1
        cenario_state = copy.deepcopy(base_state_template)

        for i, resultado in enumerate(cenario):
            pts_c, pts_f, saldo_c, _ = resultado
            tc = jogos_para_processar[i]['time_casa']
            tf = jogos_para_processar[i]['time_fora']
            saldo_f = -saldo_c

            if tc in cenario_state:
                cenario_state[tc]['p'] += pts_c
                cenario_state[tc]['sg'] += saldo_c
                if pts_c == 3: cenario_state[tc]['v'] += 1
            if tf in cenario_state:
                cenario_state[tf]['p'] += pts_f
                cenario_state[tf]['sg'] += saldo_f
                if pts_f == 3: cenario_state[tf]['v'] += 1

        grupos_temp = {'A': [], 'B': [], 'C': []}
        for t_dados in cenario_state.values():
            grupos_temp[t_dados['grupo']].append(t_dados)

        for g in keys_groups:
            grupos_temp[g].sort(key=key_func, reverse=True)

        ranking_final = []
        ranking_final.extend(sorted([grupos_temp[g][0] for g in keys_groups if len(grupos_temp[g])>0], key=key_func, reverse=True))
        ranking_final.extend(sorted([grupos_temp[g][1] for g in keys_groups if len(grupos_temp[g])>1], key=key_func, reverse=True))
        ranking_final.extend(sorted([grupos_temp[g][2] for g in keys_groups if len(grupos_temp[g])>2], key=key_func, reverse=True))
        ranking_final.extend(sorted([grupos_temp[g][3] for g in keys_groups if len(grupos_temp[g])>3], key=key_func, reverse=True))

        for rank_idx, time_data in enumerate(ranking_final):
            pos = rank_idx + 1
            nm = time_data['nome']
            if pos <= 4: stats_counts[nm]['semi'] += 1
            elif 5 <= pos <= 8: stats_counts[nm]['inconf'] += 1
            elif pos >= 11: stats_counts[nm]['rebaix'] += 1

    resultado_final = []
    div = total_cenarios if total_cenarios > 0 else 1

    for time, contagens in stats_counts.items():
        if div > 0 and (contagens['semi'] > 0 or contagens['inconf'] > 0 or contagens['rebaix'] > 0):
            logo_url = tabela_base[time].get('logo', '')
            resultado_final.append({
                'time': time, 'logo': logo_url,
                'semi': round((contagens['semi'] / div) * 100, 1),
                'inconf': round((contagens['inconf'] / div) * 100, 1),
                'rebaix': round((contagens['rebaix'] / div) * 100, 1)
            })

    return resultado_final

@app.route('/')
@login_required # <--- ISSO TRANCA A PÁGINA
def index():
    raw_games = buscar_dados_brutos()
    logos = carregar_mapa_logos()

    tabela_calc, rodadas_dict, jogos_abertos = processar_tabela_base(raw_games, {}, logos)
    classificacao_geral, grupos = ordenar_ranking_final(tabela_calc)

    probs = []

    r_atual = 1
    for r in sorted(rodadas_dict.keys()):
        if any(j['gols_casa'] is None for j in rodadas_dict[r]):
            r_atual = r
            break

    return render_template('index.html',
                           grupos=grupos,
                           classificacao_geral=classificacao_geral,
                           rodadas=rodadas_dict,
                           rodada_atual_num=r_atual,
                           probabilidades=probs,
                           user=current_user)

@app.route('/api/atualizar_tabela', methods=['POST'])
@login_required # Opcional: proteger a API também
def api_atualizar():
    data = request.json
    sims_user = data.get('simulacoes', {})
    calcular_agora = data.get('calcular_probabilidade', False)

    raw_games = buscar_dados_brutos()
    logos = carregar_mapa_logos()

    tabela_calc, _, jogos_abertos = processar_tabela_base(raw_games, sims_user, logos)
    classificacao_geral, grupos = ordenar_ranking_final(tabela_calc)

    probabilidades = []
    if calcular_agora:
        probabilidades = calcular_probabilidade_exata(tabela_calc, jogos_abertos)

    return jsonify({
        'geral': classificacao_geral,
        'grupos': grupos,
        'probabilidades': probabilidades
    })

if __name__ == '__main__':
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', 8080))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host=host, port=port, debug=debug)