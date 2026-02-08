from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, login_required, UserMixin, current_user
from werkzeug.security import check_password_hash, generate_password_hash
import mysql.connector
import os
# Importando do novo arquivo centralizador
from database import get_db_connection

auth_bp = Blueprint('auth', __name__)

class User(UserMixin):
    def __init__(self, id, username):
        self.id = id
        self.username = username

def load_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT username FROM users WHERE username = %s", (user_id,))
        user_data = cursor.fetchone()
        if user_data:
            return User(id=user_data['username'], username=user_data['username'])
    except Exception as e:
        print(f"Erro ao carregar usuário: {e}")
    finally:
        conn.close()
    return None

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT username, password_hash FROM users WHERE username = %s", (username,))
        user_data = cursor.fetchone()
        conn.close()

        if user_data and check_password_hash(user_data['password_hash'], password):
            user = User(id=user_data['username'], username=user_data['username'])
            login_user(user)
            next_page = request.args.get('next')
            return redirect(next_page or url_for('index'))
        else:
            flash('Usuário ou senha incorretos.', 'danger')

    return render_template('login.html')

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        if not username or not password:
            flash('Preencha todos os campos.', 'warning')
            return render_template('register.html')

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        try:
            cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
            if cursor.fetchone():
                flash('Este nome de usuário já está em uso.', 'danger')
            else:
                senha_hash = generate_password_hash(password)
                cursor.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s)", (username, senha_hash))
                conn.commit()

                user = User(id=username, username=username)
                login_user(user)

                flash('Conta criada com sucesso!', 'success')
                return redirect(url_for('index'))
        except mysql.connector.Error as err:
            flash(f'Erro no banco de dados: {err}', 'danger')
        finally:
            conn.close()

    return render_template('register.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Você saiu do sistema.', 'info')
    return redirect(url_for('auth.login'))
