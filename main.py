"""
MADHU MISHRA — Facebook Message & Image Sender
Pterodactyl-ready · Non-stop · Auto-resume · IST · Silent · Cleaner

Startup Command (Pterodactyl Startup tab):
bash -c "pip install -r requirements.txt && while true; do gunicorn -w 1 --threads 20 --timeout 120 -b 0.0.0.0:{{SERVER_PORT}} app:app; sleep 5; done"

Startup Detection:
{"done": ["MADHU MISHRA SERVER IS RUNNING"]}
"""

import os
import sys
import time
import json
import random
import string
import sqlite3
import resource
import requests
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from threading import Thread, Event, Lock
from collections import deque
from flask import Flask, request, jsonify

# ============================ CONFIG ============================
DB_PATH          = os.environ.get('DB_PATH', 'script2_tasks.db')
KEEP_ALIVE_URL   = os.environ.get('KEEP_ALIVE_URL', '')
LOG_STDOUT       = os.environ.get('LOG_STDOUT', '0') == '1'
LOG_BUFFER_MAX   = 1000
LOG_WINDOW_SECS  = 600
RATE_LIMIT_PER_M = 2

IST = timezone(timedelta(hours=5, minutes=30))
SERVER_START_TIME = time.time()

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['DEBUG'] = False

# ============================ LOG BUFFER ============================
log_buffer = deque(maxlen=LOG_BUFFER_MAX)
log_lock = Lock()

def log_event(level, msg, task_key=''):
    with log_lock:
        log_buffer.append((time.time(), level.upper(), msg, task_key or ''))
    if LOG_STDOUT and level.upper() in ('ERROR', 'CRITICAL'):
        try: print(f"[{level}] {msg}", flush=True)
        except: pass

# ============================ IST HELPERS ============================
def now_ist():
    return datetime.now(IST)

def now_ist_str():
    return now_ist().strftime('%Y-%m-%d %I:%M:%S %p')

def ts_ist_str(ts):
    return datetime.fromtimestamp(ts, IST).strftime('%Y-%m-%d %I:%M:%S %p')

def ts_ist_hm(ts):
    return datetime.fromtimestamp(ts, IST).strftime('%H:%M:%S')

def fmt_uptime(secs):
    secs = int(secs)
    return f"{secs//86400}d {(secs%86400)//3600}h {(secs%3600)//60}m {secs%60}s"

def get_process_mem_mb():
    try:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(rss / (1024 * 1024), 1) if sys.platform == 'darwin' else round(rss / 1024, 1)
    except Exception:
        return 0.0

# ============================ DB ============================
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA journal_size_limit=16777216')
    conn.execute('PRAGMA busy_timeout=30000')
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def db_vacuum():
    try:
        c = sqlite3.connect(DB_PATH, timeout=120)
        c.execute('VACUUM')
        c.close()
        log_event('INFO', 'DB vacuum done')
    except Exception as e:
        log_event('WARN', f'vacuum failed: {e}')

def init_db():
    with get_db() as conn:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS tasks (
                task_key        TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                hatersname      TEXT NOT NULL,
                lastname        TEXT NOT NULL,
                time_interval   INTEGER NOT NULL,
                tokens_json     TEXT NOT NULL,
                messages_json   TEXT NOT NULL,
                image_blob      BLOB,
                image_filename  TEXT,
                status          TEXT NOT NULL DEFAULT 'running',
                start_time      TEXT NOT NULL,
                last_message    TEXT NOT NULL,
                message_count   INTEGER NOT NULL DEFAULT 0,
                message_index   INTEGER NOT NULL DEFAULT 0,
                token_index     INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

            CREATE TABLE IF NOT EXISTS token_usage (
                token     TEXT NOT NULL,
                timestamp REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tu ON token_usage(token, timestamp);
        ''')

def db_save_task(t):
    with get_db() as conn:
        conn.execute('''
            INSERT INTO tasks (task_key, conversation_id, hatersname, lastname,
                time_interval, tokens_json, messages_json, image_blob, image_filename, status, start_time,
                last_message, message_count, message_index, token_index)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(task_key) DO UPDATE SET
                status = excluded.status
        ''', (
            t['task_key'], t['conversation_id'], t['hatersname'], t['lastname'],
            t['time_interval'], t['tokens_json'], t['messages_json'],
            t['image_blob'], t['image_filename'],
            t['status'], t['start_time'], t['last_message'], 0, 0, 0
        ))

def db_update_progress(task_key, message_count, last_message, message_index, token_index):
    with get_db() as conn:
        conn.execute('''UPDATE tasks SET message_count=?, last_message=?,
                        message_index=?, token_index=? WHERE task_key=?''',
                     (message_count, last_message, message_index, token_index, task_key))

def db_get_task(task_key):
    with get_db() as conn:
        r = conn.execute('SELECT * FROM tasks WHERE task_key=?', (task_key,)).fetchone()
    return dict(r) if r else None

def db_running_tasks():
    with get_db() as conn:
        rs = conn.execute("SELECT * FROM tasks WHERE status='running'").fetchall()
    return [dict(r) for r in rs]

def db_set_status(task_key, status):
    with get_db() as conn:
        conn.execute('UPDATE tasks SET status=? WHERE task_key=?', (status, task_key))

def db_delete_task(task_key):
    with get_db() as conn:
        conn.execute('DELETE FROM tasks WHERE task_key=?', (task_key,))

# ============================ RATE LIMIT ============================
def check_rate_limit(token):
    now = time.time()
    with get_db() as conn:
        conn.execute('DELETE FROM token_usage WHERE timestamp < ?', (now - 3600,))
        n = conn.execute('SELECT COUNT(*) FROM token_usage WHERE token=? AND timestamp>=?',
                         (token, now - 60)).fetchone()[0]
    return n >= RATE_LIMIT_PER_M

def update_token_usage(token):
    with get_db() as conn:
        conn.execute('INSERT INTO token_usage (token,timestamp) VALUES (?,?)', (token, time.time()))

def clear_token_usage(token):
    with get_db() as conn:
        conn.execute('DELETE FROM token_usage WHERE token=?', (token,))

# ============================ HELPERS ============================
def gen_task_key():
    return ''.join(random.choices(string.ascii_lowercase, k=10))

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0',
}

def check_token_validity(token):
    try:
        r = requests.get(
            f"https://graph.facebook.com/v17.0/me?access_token={token}&fields=id,name,email,picture",
            timeout=10)
        if r.status_code != 200:
            return {"valid": False, "error": f"HTTP {r.status_code}"}
        d = r.json()
        if 'id' not in d or 'name' not in d:
            return {"valid": False, "error": "Invalid token response"}
        return {
            "valid": True,
            "user_id": d.get('id', 'N/A'),
            "name": d.get('name', 'N/A'),
            "email": d.get('email', 'N/A'),
            "picture": d.get('picture', {}).get('data', {}).get('url', '')
        }
    except Exception as e:
        return {"valid": False, "error": str(e)}

def extract_messenger_chat_groups(token):
    try:
        r = requests.get(
            f"https://graph.facebook.com/v17.0/me/conversations?access_token={token}&fields=id,name,participants&limit=100",
            timeout=15)
        if r.status_code != 200:
            return {"success": False, "error": f"HTTP {r.status_code}", "chat_groups": [], "total_chats": 0}
        chats = []
        for c in r.json().get('data', []):
            chats.append({
                'thread_id': c.get('id', 'N/A'),
                'name': c.get('name', 'Unnamed Chat'),
                'participants_count': len(c.get('participants', {}).get('data', []))
                    if c.get('participants') else 0
            })
        return {"success": True, "chat_groups": chats, "total_chats": len(chats)}
    except Exception as e:
        return {"success": False, "error": str(e), "chat_groups": [], "total_chats": 0}

# ============================ WORKER CORE ============================
active_workers = set()
workers_lock = Lock()
stop_events = {}

def run_worker(task_key, stop_event, db_task):
    try:
        access_tokens = json.loads(db_task['tokens_json']) or ['']
        messages = json.loads(db_task['messages_json']) or [' ']
    except Exception as e:
        log_event('ERROR', f'payload parse failed: {e}', task_key)
        return

    thread_id    = db_task['conversation_id']
    hatersname   = db_task['hatersname']
    lastname     = db_task['lastname']
    time_interval = max(1, int(db_task['time_interval'] or 30))
    image_blob   = db_task.get('image_blob')
    image_filename = db_task.get('image_filename') or 'image.jpg'

    msg_idx   = int(db_task.get('message_index') or 0) % len(messages)
    tok_idx   = int(db_task.get('token_index') or 0) % len(access_tokens)
    msg_count = int(db_task.get('message_count') or 0)
    last_time = db_task.get('last_message') or now_ist_str()

    iter_counter = 0
    while not stop_event.is_set():
        iter_counter += 1
        if iter_counter % 5 == 0:
            cur = db_get_task(task_key)
            if not cur or cur['status'] != 'running':
                stop_event.set()
                return

        message_text = messages[msg_idx]
        access_token = access_tokens[tok_idx]

        try:
            if check_rate_limit(access_token):
                log_event('INFO', f'rate limit hit, 5m pause', task_key)
                for _ in range(60):
                    if stop_event.wait(timeout=5): return
                clear_token_usage(access_token)
                continue
        except Exception as e:
            log_event('WARN', f'rate check err: {e}', task_key)
            if stop_event.wait(timeout=10): return
            continue

        message = f"{hatersname} {message_text} {lastname}".strip()
        api_url = f'https://graph.facebook.com/v17.0/t_{thread_id}/'

        try:
            if image_blob:
                files = {
                    'source': (image_filename, image_blob, 'image/jpeg')
                }
                data = {
                    'access_token': access_token,
                    'message': message
                }
                resp = requests.post(api_url, data=data, files=files, headers=HEADERS, timeout=30)
            else:
                data = {
                    'access_token': access_token,
                    'message': message
                }
                resp = requests.post(api_url, data=data, headers=HEADERS, timeout=30)

            if resp.status_code == 200:
                try: update_token_usage(access_token)
                except: pass
                msg_count += 1
                last_time = now_ist_str()
                if iter_counter % 10 == 0 or msg_count % 10 == 0:
                    log_event('INFO', f'sent #{msg_count}', task_key)
            else:
                log_event('WARN', f'HTTP {resp.status_code}: {resp.text}', task_key)
        except Exception as e:
            log_event('WARN', f'send err: {e}', task_key)

        tok_idx += 1
        if tok_idx >= len(access_tokens):
            tok_idx = 0
            msg_idx += 1
            if msg_idx >= len(messages):
                msg_idx = 0
                try: db_update_progress(task_key, msg_count, last_time, msg_idx, tok_idx)
                except: pass
                log_event('INFO', f'cycle complete, sent={msg_count}', task_key)
                if stop_event.wait(timeout=20): return
                continue

        try: db_update_progress(task_key, msg_count, last_time, msg_idx, tok_idx)
        except Exception as e:
            log_event('WARN', f'db save err: {e}', task_key)

        if stop_event.wait(timeout=time_interval):
            return

def worker_supervisor(task_key):
    try:
        while True:
            cur = db_get_task(task_key)
            if not cur:
                break
            if cur['status'] != 'running':
                break

            stop_event = stop_events.get(task_key)
            if not stop_event or stop_event.is_set():
                if stop_event and stop_event.is_set():
                    break
                stop_event = Event()
                stop_events[task_key] = stop_event

            try:
                run_worker(task_key, stop_event, cur)
            except Exception as e:
                log_event('ERROR', f'supervisor crash: {e}', task_key)
                if stop_event.wait(timeout=5): break
                continue

            if stop_event.is_set():
                break
            log_event('WARN', 'worker exited, restarting in 2s', task_key)
            if stop_event.wait(timeout=2): break
    finally:
        with workers_lock:
            active_workers.discard(task_key)

def ensure_worker(task_key):
    with workers_lock:
        if task_key in active_workers:
            return
        active_workers.add(task_key)
    Thread(target=worker_supervisor, args=(task_key,),
           daemon=True, name=f'w-{task_key}').start()

def resume_all_on_startup():
    running = db_running_tasks()
    log_event('INFO', f'resuming {len(running)} tasks from DB')
    for t in running:
        try:
            ensure_worker(t['task_key'])
        except Exception as e:
            log_event('ERROR', f'resume failed {t["task_key"]}: {e}')

# ============================ CLEANER ============================
def cleaner_loop():
    cycle = 0
    while True:
        try:
            time.sleep(60)
            cycle += 1

            try:
                with get_db() as conn:
                    conn.execute('DELETE FROM token_usage WHERE timestamp < ?',
                                 (time.time() - 3600,))
            except Exception:
                pass

            if cycle % 5 == 0:
                try:
                    with get_db() as conn:
                        conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                except Exception:
                    pass

            if cycle % (60 * 24 * 7) == 0:
                db_vacuum()
        except Exception:
            pass

def keep_alive_loop():
    while True:
        try:
            requests.get(KEEP_ALIVE_URL or 'http://localhost:5000/ping', timeout=5)
        except Exception:
            pass
        time.sleep(60)

# ============================ ROUTES ============================
@app.route('/')
def index():
    return HTML_TEMPLATE

@app.route('/ping')
def ping():
    return 'pong'

@app.route('/health')
def health():
    return jsonify({'ok': True, 'uptime_secs': int(time.time() - SERVER_START_TIME)})

@app.route('/server_status')
def server_status():
    uptime_secs = int(time.time() - SERVER_START_TIME)
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        running = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='running'").fetchone()[0]
    return jsonify({
        'uptime': fmt_uptime(uptime_secs),
        'uptime_secs': uptime_secs,
        'start_time_ist': ts_ist_str(SERVER_START_TIME),
        'now_ist': now_ist_str(),
        'active_workers': len(active_workers),
        'total_tasks': total,
        'running_tasks': running,
        'memory_mb': get_process_mem_mb(),
        'log_count': len(log_buffer),
    })

@app.route('/logs')
def logs():
    cutoff = time.time() - LOG_WINDOW_SECS
    with log_lock:
        items = [e for e in log_buffer if e[0] >= cutoff]
    return jsonify({
        'logs': [
            {'time': ts_ist_hm(t), 'level': l, 'msg': m, 'task': k}
            for t, l, m, k in items
        ],
        'window_secs': LOG_WINDOW_SECS,
    })

@app.route('/start_task', methods=['POST'])
def start_task():
    try:
        data = request.form

        tokens = []
        if 'token_file' in request.files and request.files['token_file'].filename:
            tokens = [l.strip() for l in request.files['token_file'].read().decode('utf-8').split('\n') if l.strip()]
        elif data.get('single_token'):
            tokens = [data['single_token'].strip()]
        if not tokens:
            return jsonify({'error': 'No valid tokens'})

        messages = []
        if 'message_file' in request.files and request.files['message_file'].filename:
            messages = [l.strip() for l in request.files['message_file'].read().decode('utf-8').split('\n') if l.strip()]
        if not messages:
            return jsonify({'error': 'No messages'})

        image_blob = None
        image_filename = None
        if 'image_file' in request.files and request.files['image_file'].filename:
            img_file = request.files['image_file']
            image_blob = img_file.read()
            image_filename = img_file.filename

        task_key = gen_task_key()
        start_str = now_ist_str()

        db_save_task({
            'task_key': task_key,
            'conversation_id': data['conversation_id'],
            'hatersname': data['hatersname'],
            'lastname': data['lastname'],
            'time_interval': int(data['time_interval']),
            'tokens_json': json.dumps(tokens),
            'messages_json': json.dumps(messages),
            'image_blob': image_blob,
            'image_filename': image_filename,
            'status': 'running',
            'start_time': start_str,
            'last_message': start_str,
        })

        ensure_worker(task_key)
        log_event('INFO', f'task started ({len(tokens)} tok, {len(messages)} msg)', task_key)
        return jsonify({'task_key': task_key})
    except Exception as e:
        log_event('ERROR', f'start_task failed: {e}')
        return jsonify({'error': str(e)})

@app.route('/check_status', methods=['POST'])
def check_status():
    t = db_get_task(request.form.get('task_key'))
    if not t:
        return jsonify({'error': 'Task not found'})
    return jsonify({
        'conversation_id': t['conversation_id'],
        'start_time': t['start_time'],
        'last_message': t['last_message'],
        'token_count': len(json.loads(t['tokens_json'])),
        'message_count': t['message_count'],
        'status': t['status'],
        'now_ist': now_ist_str(),
    })

@app.route('/list_tasks')
def list_tasks():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT task_key, conversation_id, hatersname, lastname, status,
                   start_time, last_message, message_count, time_interval
            FROM tasks ORDER BY start_time DESC
        """).fetchall()
    return jsonify({'tasks': [dict(r) for r in rows]})

@app.route('/control_task', methods=['POST'])
def control_task():
    task_key = request.form.get('task_key')
    action = request.form.get('action')
    if not db_get_task(task_key):
        return jsonify({'error': 'Task not found'})

    if action == 'stop':
        ev = stop_events.get(task_key)
        if ev: ev.set()
        db_set_status(task_key, 'stopped')
        log_event('INFO', f'manual stop', task_key)

    elif action == 'resume':
        db_set_status(task_key, 'running')
        ev = stop_events.get(task_key)
        if ev: ev.clear()
        else: stop_events[task_key] = Event()
        ensure_worker(task_key)
        log_event('INFO', f'manual resume', task_key)

    elif action == 'delete':
        ev = stop_events.get(task_key)
        if ev: ev.set()
        db_delete_task(task_key)
        with workers_lock:
            active_workers.discard(task_key)
        stop_events.pop(task_key, None)
        log_event('INFO', f'manual delete', task_key)

    return jsonify({'success': True})

@app.route('/check_tokens', methods=['POST'])
def check_tokens():
    tokens = []
    if 'token_file' in request.files and request.files['token_file'].filename:
        tokens = [l.strip() for l in request.files['token_file'].read().decode('utf-8').split('\n') if l.strip()]
    elif request.form.get('single_token'):
        tokens = [request.form['single_token'].strip()]
    if not tokens:
        return jsonify({'error': 'No tokens'})

    results, valid, invalid = [], [], []
    for tk in tokens:
        if not tk or len(tk) < 10:
            results.append({'valid': False, 'token': tk, 'error': 'too short'})
            invalid.append(tk); continue
        r = check_token_validity(tk); r['token'] = tk
        results.append(r)
        (valid if r['valid'] else invalid).append(tk)

    return jsonify({
        'results': results,
        'summary': {'total': len(tokens), 'valid': len(valid), 'invalid': len(invalid)},
        'valid_tokens': valid,
        'invalid_tokens': invalid,
    })

@app.route('/extract_messenger_chats', methods=['POST'])
def extract_messenger_chats():
    tk = request.form.get('token')
    if not tk: return jsonify({'error': 'No token'})
    tc = check_token_validity(tk)
    if not tc['valid']: return jsonify({'error': f'Invalid: {tc.get("error")}'})
    return jsonify({'token_info': tc, 'messenger_chats': extract_messenger_chat_groups(tk)})

# ============================ HTML ============================
HTML_TEMPLATE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MADHU MISHRA — FB Sender</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Poppins',sans-serif;background:url('https://i.ibb.co/XrTgznxD/1773997616608.jpg') no-repeat center center fixed;background-size:cover;min-height:100vh;padding:20px;color:#fff;text-shadow:2px 2px 4px rgba(0,0,0,.8)}
.header{text-align:center;margin-bottom:20px;padding:20px;background:rgba(0,0,0,.7);border-radius:15px;box-shadow:0 10px 30px rgba(0,0,0,.5)}
h1{color:#ff4081;font-weight:700;font-size:2.4rem;margin-bottom:6px;text-shadow:0 0 20px #ff4081}
.subtitle{color:#fff;font-size:1rem;font-weight:300}
.status-bar{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:20px;background:rgba(0,0,0,.85);border-radius:12px;padding:14px 18px;box-shadow:0 5px 20px rgba(0,0,0,.5);font-size:.85rem;align-items:center}
.status-bar .item{flex:1;min-width:150px;text-align:center}
.status-bar .label{color:#ff4081;font-size:.7rem;text-transform:uppercase;letter-spacing:1px;display:block;margin-bottom:2px}
.status-bar .value{color:#fff;font-weight:600;font-size:.95rem}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#28a745;margin-right:6px;box-shadow:0 0 8px #28a745;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.tab-container{margin-bottom:20px}
.tabs{display:flex;margin-bottom:20px;background:rgba(0,0,0,.8);border-radius:12px;padding:5px;box-shadow:0 5px 15px rgba(0,0,0,.5);flex-wrap:wrap}
.tab{flex:1;padding:12px 16px;background:transparent;border:none;color:#fff;cursor:pointer;border-radius:10px;font-weight:500;font-size:.85rem;transition:all .3s ease;text-align:center;min-width:120px}
.tab.active{background:#ff4081;box-shadow:0 3px 10px rgba(255,64,129,.5)}
.tab:hover:not(.active){background:rgba(255,64,129,.3)}
.tab-content{display:none;padding:22px;background:rgba(0,0,0,.85);border-radius:12px;box-shadow:0 5px 20px rgba(0,0,0,.5);border:1px solid rgba(255,255,255,.1)}
.tab-content.active{display:block}
.form-group{margin-bottom:18px}
label{display:block;margin-bottom:6px;font-weight:600;color:#ff4081;font-size:.9rem}
input[type=text],input[type=number],textarea{width:100%;padding:11px 14px;border:2px solid rgba(255,255,255,.3);border-radius:8px;font-size:.95rem;background:rgba(0,0,0,.7);color:#fff;font-family:'Poppins',sans-serif}
input:focus,textarea:focus{outline:none;border-color:#ff4081;background:rgba(0,0,0,.9);box-shadow:0 0 0 3px rgba(255,64,129,.3)}
.file-input-wrapper{position:relative;width:100%}
.file-input{background:linear-gradient(135deg,#667eea,#764ba2);padding:11px 20px;border-radius:8px;cursor:pointer;text-align:center;font-weight:500;display:block;width:100%;box-shadow:0 4px 12px rgba(102,126,234,.4);position:relative;overflow:hidden}
.file-input:hover{transform:translateY(-2px)}
.file-input input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer}
.btn-group{display:flex;gap:10px;flex-wrap:wrap;margin-top:20px}
button{background:linear-gradient(135deg,#ff4081,#e91e63);color:#fff;padding:11px 22px;border:none;border-radius:8px;cursor:pointer;font-weight:600;font-size:.9rem;transition:all .3s ease;box-shadow:0 4px 12px rgba(255,64,129,.4);flex:1;min-width:110px;font-family:'Poppins',sans-serif}
button:hover{transform:translateY(-2px);box-shadow:0 6px 20px rgba(255,64,129,.6)}
button.secondary{background:linear-gradient(135deg,#667eea,#764ba2)}
button.danger{background:linear-gradient(135deg,#ff6b6b,#ee5a24)}
button.success{background:linear-gradient(135deg,#00b894,#00a085)}
.copy-btn{background:linear-gradient(135deg,#2196f3,#1976d2);padding:5px 10px;font-size:.75rem;box-shadow:0 2px 8px rgba(33,150,243,.4);flex:none;min-width:auto}
.result{margin-top:20px;padding:16px;border-radius:10px;display:none;background:rgba(0,0,0,.7);border:1px solid rgba(255,255,255,.2)}
.success-box{background:rgba(40,167,69,.2);border:2px solid #28a745;color:#d4edda}
.error-box{background:rgba(220,53,69,.2);border:2px solid #dc3545;color:#f8d7da}
.status-info{background:rgba(33,150,243,.2);border:2px solid #2196f3;padding:16px;border-radius:10px;margin-top:15px}
.token-result{margin-bottom:12px;padding:12px;border-radius:8px;background:rgba(0,0,0,.6)}
.valid{border-left:4px solid #28a745}.invalid{border-left:4px solid #dc3545}
.chat-list{margin-top:15px;max-height:400px;overflow-y:auto;border:2px solid rgba(255,255,255,.2);padding:12px;background:rgba(0,0,0,.6);border-radius:8px}
.chat-item{padding:10px;border-bottom:1px solid rgba(255,255,255,.1);display:flex;justify-content:space-between;align-items:center;gap:10px}
.chat-item:hover{background:rgba(255,255,255,.08)}
.small-text{font-size:.8rem;color:rgba(255,255,255,.7);margin-top:4px}
.badge{display:inline-block;background:linear-gradient(135deg,#ffd700,#ffed4e);color:#000;padding:2px 8px;border-radius:12px;font-size:.68rem;font-weight:600;margin-left:6px}
#logs-box{background:#0a0a0a;border:1px solid #222;border-radius:8px;padding:12px;font-family:monospace;font-size:.8rem;max-height:520px;overflow-y:auto;line-height:1.55}
.log-line{padding:2px 0;border-bottom:1px solid rgba(255,255,255,.05);word-break:break-word}
.log-line .t{color:#888;margin-right:8px}
.log-line .lv{font-weight:600;margin-right:8px}
.log-line .lv.INFO{color:#4fc3f7}
.log-line .lv.WARN{color:#ffb74d}
.log-line .lv.ERROR{color:#ef5350}
.log-line .lv.CRITICAL{color:#e91e63}
.log-line .task{color:#ba68c8;margin-right:6px}
.dev{text-align:center;margin-top:30px;color:rgba(255,255,255,.7);font-style:italic;padding-top:15px;border-top:1px solid rgba(255,255,255,.2);font-size:.85rem}
@media(max-width:768px){body{padding:10px}h1{font-size:1.8rem}.tabs{flex-direction:column}.btn-group{flex-direction:column}button{width:100%}}
</style>
</head>
<body>

<div class="header">
  <h1>🔥 MADHU MISHRA</h1>
  <div class="subtitle">Professional Facebook Message & Image Sender · Non-Stop</div>
</div>

<div class="status-bar" id="statusBar">
  <div class="item"><span class="label">Uptime</span><span class="value" id="sUptime">—</span></div>
  <div class="item"><span class="label">Server Started (IST)</span><span class="value" id="sStart">—</span></div>
  <div class="item"><span class="label">Now (IST)</span><span class="value" id="sNow">—</span></div>
  <div class="item"><span class="label">Workers</span><span class="value"><span class="dot"></span><span id="sWorkers">0</span></span></div>
  <div class="item"><span class="label">Tasks (run/total)</span><span class="value" id="sTasks">0/0</span></div>
  <div class="item"><span class="label">RAM</span><span class="value" id="sMem">—</span></div>
</div>

<div class="tab-container">
<div class="tabs">
  <div class="tab active" onclick="switchTab('logs',this)">📡 Live Logs (10m)</div>
  <div class="tab" onclick="switchTab('message-sender',this)">💬 Message & Image Sender</div>
  <div class="tab" onclick="switchTab('token-checker',this)">🔑 Token Checker</div>
  <div class="tab" onclick="switchTab('chat-extractor',this)">📱 Chat Extractor</div>
  <div class="tab" onclick="switchTab('status-check',this)">📊 Status / Control</div>
</div>

<div id="logs" class="tab-content active">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
    <h3 style="color:#ff4081">📡 Live Logs (last 10 minutes)</h3>
    <div class="small-text" id="logCount">—</div>
  </div>
  <div id="logs-box"></div>
</div>

<div id="message-sender" class="tab-content">
  <form id="messageForm" enctype="multipart/form-data">
    <div class="form-group">
      <label>🔑 Access Token <span class="badge">Required</span></label>
      <input type="text" name="single_token" placeholder="Enter token...">
      <div class="small-text">OR</div>
      <div class="file-input-wrapper"><div class="file-input">📁 Upload Token File<input type="file" name="token_file" accept=".txt"></div></div>
      <div class="small-text">.txt file, one token per line</div>
    </div>
    <div class="form-group"><label>💬 Conversation ID <span class="badge">Required</span></label><input type="text" name="conversation_id" required></div>
    <div class="form-group"><label>👤 Haters Name <span class="badge">Required</span></label><input type="text" name="hatersname" required></div>
    <div class="form-group"><label>📛 Last Name <span class="badge">Required</span></label><input type="text" name="lastname" required></div>
    <div class="form-group"><label>⏱️ Time Interval (seconds) <span class="badge">Required</span></label><input type="number" name="time_interval" value="30" min="1" required></div>
    <div class="form-group"><label>📝 Messages File <span class="badge">Required</span></label><div class="file-input-wrapper"><div class="file-input">📁 Upload NP File<input type="file" name="message_file" accept=".txt" required></div></div><div class="small-text">.txt file, one message per line</div></div>
    <div class="form-group"><label>🖼️ Image File <span class="badge" style="background:#00b894;color:#fff">Optional</span></label><div class="file-input-wrapper"><div class="file-input" style="background:linear-gradient(135deg,#00b894,#00a085)">📁 Upload Image (JPG/PNG)<input type="file" name="image_file" accept="image/*"></div></div><div class="small-text">Agar image upload karoge toh text ke sath image bhi jayegi</div></div>
    <div class="btn-group"><button type="submit">🚀 Start Task</button></div>
  </form>
  <div id="taskResult" class="result"></div>
</div>

<div id="token-checker" class="tab-content">
  <form id="tokenForm" enctype="multipart/form-data">
    <div class="form-group">
      <label>🔑 Access Tokens</label>
      <input type="text" name="single_token" placeholder="Enter single token...">
      <div class="small-text" style="margin:10px 0;text-align:center;font-weight:bold">OR</div>
      <div class="file-input-wrapper"><div class="file-input">📁 Upload .txt<input type="file" name="token_file" accept=".txt"></div></div>
    </div>
    <div class="btn-group"><button type="submit">🔍 Check Tokens</button></div>
  </form>
  <div id="tokenResult"></div>
</div>

<div id="chat-extractor" class="tab-content">
  <form id="chatExtractForm">
    <div class="form-group">
      <label>🔑 Enter Valid Facebook Token <span class="badge">Required</span></label>
      <input type="text" name="token" placeholder="Enter EAAD token..." required>
    </div>
    <div class="btn-group"><button type="submit">🚀 Extract Messenger Chats</button></div>
  </form>
  <div id="chatExtractResult"></div>
</div>

<div id="status-check" class="tab-content">
  <form id="statusForm">
    <div class="form-group">
      <label>🔑 Task Key <span class="badge">Required</span></label>
      <input type="text" name="task_key" placeholder="Enter task key..." required>
    </div>
    <div class="btn-group">
      <button type="submit">📊 Check Status</button>
      <button type="button" onclick="controlTask('stop')" class="danger">⏸ Stop</button>
      <button type="button" onclick="controlTask('resume')" class="success">▶ Resume</button>
      <button type="button" onclick="controlTask('delete')" class="danger">🗑 Delete</button>
    </div>
  </form>
  <div id="statusResult"></div>
</div>

</div>

<div class="dev">developer:- vampire raj mishra</div>

<script>
function switchTab(name, el){
  document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById(name).classList.add('active');
  if(el) el.classList.add('active');
}
function copyText(txt){navigator.clipboard.writeText(txt).then(()=>alert('✅ Copied')).catch(e=>console.error(e))}

async function pollStatus(){
  try{
    const r = await fetch('/server_status');
    const d = await r.json();
    document.getElementById('sUptime').textContent = d.uptime;
    document.getElementById('sStart').textContent  = d.start_time_ist;
    document.getElementById('sNow').textContent    = d.now_ist;
    document.getElementById('sWorkers').textContent= d.active_workers;
    document.getElementById('sTasks').textContent  = d.running_tasks + '/' + d.total_tasks;
    document.getElementById('sMem').textContent    = d.memory_mb + ' MB';
  }catch(e){}
}
setInterval(pollStatus, 3000); pollStatus();

async function pollLogs(){
  try{
    const r = await fetch('/logs');
    const d = await r.json();
    const box = document.getElementById('logs-box');
    if(!d.logs.length){
      box.innerHTML = '<div class="log-line" style="color:#666">No logs in the last 10 minutes.</div>';
    } else {
      box.innerHTML = d.logs.map(l=>{
        const tk = l.task ? '<span class="task">['+l.task+']</span>' : '';
        return '<div class="log-line"><span class="t">'+l.time+'</span><span class="lv '+l.level+'">'+l.level+'</span>'+tk+escapeHtml(l.msg)+'</div>';
      }).join('');
      box.scrollTop = box.scrollHeight;
    }
    document.getElementById('logCount').textContent = d.logs.length + ' entries (10m window)';
  }catch(e){}
}
function escapeHtml(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
setInterval(pollLogs, 3000); pollLogs();

document.getElementById('messageForm').addEventListener('submit', async function(e){
  e.preventDefault();
  const fd = new FormData(this);
  const btn = this.querySelector('button[type=submit]'); const orig = btn.innerHTML;
  btn.innerHTML='⏳ Starting...'; btn.disabled=true;
  try{
    const r = await fetch('/start_task',{method:'POST',body:fd});
    const d = await r.json();
    const div = document.getElementById('taskResult'); div.style.display='block';
    if(d.task_key){
      div.className='result success-box';
      div.innerHTML='<h3>✅ Task Started!</h3><p><strong>Task Key:</strong> <span style="color:#ff4081;font-size:1.2em">'+d.task_key+'</span></p><p class="small-text">Save this key — needed to stop/resume/delete.</p><button class="copy-btn" onclick="copyText(\''+d.task_key+'\')">📋 Copy</button>';
    } else {
      div.className='result error-box';
      div.innerHTML='❌ '+ (d.error||'Unknown');
    }
  }catch(e){document.getElementById('taskResult').innerHTML='❌ '+e;}
  finally{btn.innerHTML=orig; btn.disabled=false;}
});

document.getElementById('tokenForm').addEventListener('submit', async function(e){
  e.preventDefault();
  const fd = new FormData(this);
  const btn = this.querySelector('button[type=submit]'); const orig=btn.innerHTML;
  btn.innerHTML='⏳ Checking...'; btn.disabled=true;
  try{
    const r = await fetch('/check_tokens',{method:'POST',body:fd});
    const d = await r.json();
    let h='';
    if(d.error){h='<div class="error-box" style="padding:16px;border-radius:10px">❌ '+d.error+'</div>'}
    else{
      h+='<div class="success-box" style="padding:14px;border-radius:10px;margin-bottom:12px"><strong>Total:</strong> '+d.summary.total+' | <span style="color:#28a745">Valid:</span> '+d.summary.valid+' | <span style="color:#dc3545">Invalid:</span> '+d.summary.invalid+'</div>';
      d.results.forEach((x,i)=>{
        if(x.valid){
          h+='<div class="token-result valid"><h4>✅ Valid #'+(i+1)+'</h4><p><strong>Token:</strong> <code>'+x.token.substring(0,15)+'…</code></p><p><strong>ID:</strong> '+x.user_id+' | <strong>Name:</strong> '+x.name+'</p><button class="copy-btn" onclick="copyText(\''+x.token+'\')">📋 Copy Token</button></div>';
        } else {
          h+='<div class="token-result invalid"><h4>❌ Invalid #'+(i+1)+'</h4><p><strong>Error:</strong> '+(x.error||'invalid')+'</p></div>';
        }
      });
      if(d.valid_tokens.length){
        h+='<div class="form-group"><label>✅ Valid Tokens:</label><textarea style="height:100px;font-family:monospace">'+d.valid_tokens.join('\n')+'</textarea><button class="copy-btn" onclick="copyText(`'+d.valid_tokens.join('\n')+'`)">📋 Copy All</button></div>';
      }
    }
    document.getElementById('tokenResult').innerHTML = h;
  }catch(e){document.getElementById('tokenResult').innerHTML='❌ '+e}
  finally{btn.innerHTML=orig; btn.disabled=false;}
});

document.getElementById('chatExtractForm').addEventListener('submit', async function(e){
  e.preventDefault();
  const fd = new FormData(this);
  const btn = this.querySelector('button[type=submit]'); const orig=btn.innerHTML;
  btn.innerHTML='⏳ Extracting...'; btn.disabled=true;
  try{
    const r = await fetch('/extract_messenger_chats',{method:'POST',body:fd});
    const d = await r.json();
    const div = document.getElementById('chatExtractResult');
    if(d.error){div.innerHTML='<div class="error-box" style="padding:16px;border-radius:10px">❌ '+d.error+'</div>';}
    else {
      let h='<div class="success-box" style="padding:14px;border-radius:10px"><strong>'+d.token_info.name+'</strong> | Chats: '+d.messenger_chats.total_chats+'</div>';
      if(d.messenger_chats.chat_groups?.length){
        h+='<div class="chat-list">';
        d.messenger_chats.chat_groups.forEach(c=>{
          h+='<div class="chat-item"><div><strong>'+c.name+'</strong><br><small>UID: '+c.thread_id+' | 👥 '+c.participants_count+'</small></div><button class="copy-btn" onclick="copyText(\''+c.thread_id+'\')">📋</button></div>';
        });
        h+='</div>';
      }
      div.innerHTML=h;
    }
  }catch(e){document.getElementById('chatExtractResult').innerHTML='❌ '+e}
  finally{btn.innerHTML=orig; btn.disabled=false;}
});

document.getElementById('statusForm').addEventListener('submit', async function(e){
  e.preventDefault();
  const fd = new FormData(this);
  try{
    const r = await fetch('/check_status',{method:'POST',body:fd});
    const d = await r.json();
    const div = document.getElementById('statusResult');
    if(d.error){div.innerHTML='<div class="error-box" style="padding:14px;border-radius:10px">❌ '+d.error+'</div>';return}
    const color = d.status==='running'?'#28a745':d.status==='stopped'?'#dc3545':'#ffc107';
    div.innerHTML='<div class="status-info"><h3>📊 Task Status</h3><p><strong>Status:</strong> <span style="color:'+color+';font-weight:bold">'+d.status.toUpperCase()+'</span></p><p><strong>Conversation:</strong> '+d.conversation_id+'</p><p><strong>Started:</strong> '+d.start_time+'</p><p><strong>Last Sent:</strong> '+d.last_message+'</p><p><strong>Tokens:</strong> '+d.token_count+' | <strong>Sent:</strong> '+d.message_count+'</p><p class="small-text">Now: '+d.now_ist+'</p></div>';
  }catch(e){document.getElementById('statusResult').innerHTML='❌ '+e}
});

async function controlTask(action){
  const k = document.querySelector('input[name=task_key]').value;
  if(!k){alert('Enter task key');return}
  if(!confirm(action+' this task?')) return;
  const fd = new FormData(); fd.append('task_key',k); fd.append('action',action);
  try{
    const r = await fetch('/control_task',{method:'POST',body:fd});
    const d = await r.json();
    alert(d.success?('✅ '+action+' OK'):('❌ '+(d.error||'failed')));
    if(action==='delete'){document.getElementById('statusResult').innerHTML='';document.querySelector('input[name=task_key]').value='';}
  }catch(e){alert('❌ '+e)}
}
</script>
</body>
</html>'''

# ============================ BOOT ============================
print("MADHU MISHRA SERVER IS RUNNING", flush=True)
log_event('INFO', f'server starting at {now_ist_str()}')

init_db()
resume_all_on_startup()
Thread(target=cleaner_loop, daemon=True, name='cleaner').start()
Thread(target=keep_alive_loop, daemon=True, name='keepalive').start()

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('SERVER_PORT', os.environ.get('PORT', 5000))),
        debug=False,
        threaded=True
    )
