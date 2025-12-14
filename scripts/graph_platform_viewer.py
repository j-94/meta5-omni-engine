#!/usr/bin/env python3
"""
Meta5 Graph Platform Viewer
Real-time aggregator of the localized galaxy.
"""
import os
import time
import sys
import glob
from datetime import datetime

# ANSI Colors
COLORS = {
    'stackfile': '\033[96m', # Cyan
    'viz': '\033[95m',       # Magenta
    'reload': '\033[93m',    # Yellow
    'assertions': '\033[91m',# Red
    'eternal': '\033[92m',   # Green
    'ui': '\033[94m',        # Blue
    'RESET': '\033[0m'
}

def get_node_name(filepath):
    # logs/eternal.log -> eternal
    base = os.path.basename(filepath)
    name = base.replace('.log', '').replace('meta5_node_', '').replace('meta5_', '')
    # map to short names
    if 'stackfile' in name: return 'STACKFILE'
    if 'graph_viz' in name: return 'VIZ'
    if 'hot_reload' in name: return 'RELOAD'
    if 'runtime_assertions' in name: return 'ASSERT'
    if 'eternal_algorithms' in name: return 'ETERNAL'
    if 'symbiotic_ui' in name: return 'UI'
    return name.upper()

def get_color(node_name):
    for k, v in COLORS.items():
        if k.upper() in node_name:
            return v
    return '\033[97m' # White

def tail_files(log_dir):
    files = {}
    print(f"\033[1mMeta5 Graph Platform Live View ({log_dir})\033[0m")
    print("-" * 60)

    while True:
        # Discover new logs
        current_logs = glob.glob(os.path.join(log_dir, "*.log"))
        
        for log in current_logs:
            if log not in files:
                f = open(log, 'r')
                f.seek(0, 2) # End
                files[log] = f
                print(f"[\033[90mSYSTEM\033[0m] Found signal source: {get_node_name(log)}")

        # Read available lines
        got_data = False
        for log, f in files.items():
            pos = f.tell()
            line = f.readline()
            if line:
                node = get_node_name(log)
                color = get_color(node)
                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                
                # Format: [TIME] [NODE] -> OUTPUT
                print(f"\033[90m[{ts}]\033[0m {color}[{node:<10}]\033[0m ➔ {line.strip()}")
                got_data = True
            else:
                f.seek(pos)
        
        if not got_data:
            time.sleep(0.1)

if __name__ == "__main__":
    try:
        if not os.path.exists("logs"):
            os.mkdir("logs")
        tail_files("logs")
    except KeyboardInterrupt:
        print("\nDisconnected.")
