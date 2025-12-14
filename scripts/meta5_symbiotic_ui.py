#!/usr/bin/env python3
# META5 symbiotic UI server — broadcast simulated neural telemetry
import asyncio, websockets, random, datetime
from flask import Flask, send_from_directory
from flask_socketio import SocketIO

HOST = "0.0.0.0"
PORT = 8080

app = Flask(__name__, static_folder='../ui')
socketio = SocketIO(app, cors_allowed_origins="*")

@app.route('/')
def hud(): 
    return send_from_directory(app.static_folder, 'empire_hud.html')

def telemetry_loop():
    sig = lambda: random.choice(["ARP", "RUL", "ÆON", "VOID", "NODE", "Ω"])
    while True:
        msg = f"{datetime.datetime.utcnow().strftime('%H:%M:%S')} [{sig()}] Signal octa =  {random.gauss(1,0.2):.2f}\n"
        socketio.emit('telemetry', msg)
        socketio.sleep(random.uniform(0.05, 0.35))

if __name__ == '__main__':
    socketio.start_background_task(telemetry_loop)
    socketio.run(app, host=HOST, port=PORT)