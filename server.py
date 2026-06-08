#!/usr/bin/env python3
"""
Mini-server per Pizzeria Annarè
- Serve i file statici (HTML, CSS, JS, immagini)
- Gestisce le prenotazioni in bookings.json
- Notifica admin.html in tempo reale via Server-Sent Events
Avvio: python server.py
"""

import http.server
import json
import os
import re
import queue
import threading
import socketserver
from datetime import datetime
try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

PORT      = 8787
HOST      = '0.0.0.0'

# ── Telegram ───────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = '8541324488:AAGtJcp8YBx9832k6kUXT3fjKyfvj74M3fQ'
TELEGRAM_CHAT_ID   = '382168887'


def send_telegram(booking):
    if not REQUESTS_OK:
        print('[Telegram] libreria requests non installata — esegui: pip install requests')
        return
    try:
        data = booking.get
        date_str = booking.get('date', '')
        try:
            from datetime import date
            y, m, d = date_str.split('-')
            giorni   = ['Lunedì','Martedì','Mercoledì','Giovedì','Venerdì','Sabato','Domenica']
            mesi     = ['','Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno',
                        'Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre']
            wd       = datetime.strptime(date_str, '%Y-%m-%d').weekday()
            date_str = f"{giorni[wd]} {int(d)} {mesi[int(m)]} {y}"
        except Exception:
            pass

        note = booking.get('notes', '').strip()
        msg  = (
            f"*Nuova prenotazione*\n\n"
            f"*Nome:* {booking.get('fname','')} {booking.get('lname','')}\n"
            f"*Data:* {date_str}\n"
            f"*Orario:* {booking.get('slot','')}\n"
            f"*Persone:* {booking.get('people','')}\n"
            f"*Telefono:* {booking.get('phone','')}\n"
            + (f"*Note:* {note}\n" if note else "")
        )
        url  = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
        requests.post(url, json={
            'chat_id':    TELEGRAM_CHAT_ID,
            'text':       msg,
            'parse_mode': 'Markdown'
        }, timeout=8)
    except Exception as e:
        print(f'[Telegram] errore invio: {e}')
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, 'bookings.json')

# ── SSE: lista delle code dei client connessi ──────────────────────────────
_clients      = []
_clients_lock = threading.Lock()


def read_bookings():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def write_bookings(data):
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def cleanup_old_bookings():
    """Elimina da bookings.json tutte le prenotazioni con data < oggi."""
    today = datetime.now().date()
    all_b = read_bookings()
    filtered = []
    removed = 0
    for b in all_b:
        try:
            if datetime.strptime(b['date'], '%Y-%m-%d').date() >= today:
                filtered.append(b)
            else:
                removed += 1
        except Exception:
            filtered.append(b)
    if removed:
        write_bookings(filtered)
        print(f"[Cleanup] Rimosse {removed} prenotazioni scadute.")


def _cleanup_scheduler():
    """Thread in background: esegue cleanup a mezzanotte ogni giorno."""
    while True:
        now = datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        # prossima mezzanotte
        from datetime import timedelta
        next_midnight = midnight + timedelta(days=1)
        seconds_to_sleep = (next_midnight - now).total_seconds()
        threading.Event().wait(seconds_to_sleep)
        cleanup_old_bookings()


def notify_clients(event='new_booking'):
    """Invia un evento SSE a tutti i client admin connessi."""
    with _clients_lock:
        dead = []
        for q in _clients:
            try:
                q.put_nowait(event)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _clients.remove(q)


class Handler(http.server.SimpleHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]} {args[1]}")

    def send_json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        if self.path == '/api/bookings':
            self.send_json(200, read_bookings())

        elif self.path == '/api/events':
            # ── Server-Sent Events ──────────────────────────────────────
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            q = queue.Queue(maxsize=20)
            with _clients_lock:
                _clients.append(q)

            try:
                # Ping iniziale
                self.wfile.write(b'data: connected\n\n')
                self.wfile.flush()

                while True:
                    try:
                        event = q.get(timeout=25)   # heartbeat ogni 25 sec
                        self.wfile.write(f'data: {event}\n\n'.encode())
                        self.wfile.flush()
                    except queue.Empty:
                        # Heartbeat per tenere viva la connessione
                        self.wfile.write(b': ping\n\n')
                        self.wfile.flush()
            except Exception:
                pass
            finally:
                with _clients_lock:
                    if q in _clients:
                        _clients.remove(q)

        elif self.path == '/':
            self.send_response(302)
            self.send_header('Location', '/landing.html')
            self.end_headers()

        else:
            super().do_GET()

    def do_POST(self):
        if self.path == '/api/bookings':
            length  = int(self.headers.get('Content-Length', 0))
            body    = self.rfile.read(length)
            booking = json.loads(body.decode('utf-8'))
            all_b   = read_bookings()
            all_b.append(booking)
            write_bookings(all_b)
            notify_clients('new_booking')   # ← notifica SSE istantanea
            threading.Thread(target=send_telegram, args=(booking,), daemon=True).start()
            self.send_json(201, booking)
        else:
            self.send_json(404, {'error': 'not found'})

    def do_PATCH(self):
        m = re.match(r'^/api/bookings/(.+)$', self.path)
        if m:
            bid    = m.group(1)
            length = int(self.headers.get('Content-Length', 0))
            body   = json.loads(self.rfile.read(length).decode('utf-8'))
            all_b  = read_bookings()
            for b in all_b:
                if str(b.get('id')) == bid:
                    b.update(body)
                    write_bookings(all_b)
                    self.send_json(200, b)
                    return
            self.send_json(404, {'error': 'not found'})
        else:
            self.send_json(404, {'error': 'not found'})

    def do_DELETE(self):
        m = re.match(r'^/api/bookings/(.+)$', self.path)
        if m:
            bid   = m.group(1)
            all_b = read_bookings()
            new_b = [b for b in all_b if str(b.get('id')) != bid]
            write_bookings(new_b)
            self.send_json(200, {'deleted': bid})
        else:
            self.send_json(404, {'error': 'not found'})


class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads      = True


if __name__ == '__main__':
    cleanup_old_bookings()   # pulizia immediata all'avvio
    t = threading.Thread(target=_cleanup_scheduler, daemon=True)
    t.start()
    with ThreadedServer((HOST, PORT), Handler) as httpd:
        print(f"Server avviato su http://{HOST}:{PORT}")
        print(f"Da telefono: http://192.168.1.51:{PORT}/landing.html")
        print("Ctrl+C per fermare\n")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer fermato.")
