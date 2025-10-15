#!/usr/bin/env python3
"""
Database checker for Lab 07c
"""

import sqlite3

def check_database():
    # Check what's in the database
    with sqlite3.connect('./lab07c_threads.db') as conn:
        # Check tables
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        print('Tables:', [t[0] for t in tables])
        
        # Check checkpoints table
        if tables:
            checkpoints = conn.execute('SELECT thread_id, user_id, checkpoint_id, created_at FROM checkpoints').fetchall()
            print(f'Checkpoints found: {len(checkpoints)}')
            for checkpoint in checkpoints:
                print(f'  Thread: {checkpoint[0]}, User: {checkpoint[1]}, ID: {checkpoint[2]}, Created: {checkpoint[3]}')

if __name__ == "__main__":
    check_database()