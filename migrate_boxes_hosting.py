#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Migrate database for boxes achievements - FOR HOSTING"""

import sqlite3
import os

# Use the correct path for hosting
DB_PATH = 'data/2pcclub.db'

# Create data directory if doesn't exist
os.makedirs('data', exist_ok=True)

print(f'Connecting to {DB_PATH}...')
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print('Starting database migration...')

# 1. Add new columns to user_achievement_stats
columns_to_add = [
    'starter_pack_opened',
    'gamer_case_opened',
    'business_box_opened',
    'champion_chest_opened',
    'pro_gear_opened',
    'legend_vault_opened',
    'vip_mystery_opened'
]

for column in columns_to_add:
    try:
        cursor.execute(f'ALTER TABLE user_achievement_stats ADD COLUMN {column} INTEGER DEFAULT 0')
        print(f'✓ Added column: {column}')
    except sqlite3.OperationalError as e:
        if 'duplicate column' in str(e).lower():
            print(f'  Column already exists: {column}')
        else:
            print(f'✗ Error adding {column}: {e}')

# 2. Check if boxes achievements already exist
cursor.execute("SELECT COUNT(*) FROM achievements WHERE category LIKE 'boxes_%'")
count = cursor.fetchone()[0]

if count == 0:
    print('\nAdding box achievements...')

    achievements = [
        # STARTER PACK
        ("🎁 Первые шаги", "Открыть 10 📦 STARTER PACK", "boxes_starter", 10, "starter_pack", 1),
        ("🎁 Коллекционер стартеров", "Открыть 25 📦 STARTER PACK", "boxes_starter", 25, "starter_pack", 5),
        ("🎁 Мастер стартов", "Открыть 50 📦 STARTER PACK", "boxes_starter", 50, "gamer_case", 3),
        ("🎁 Легенда стартеров", "Открыть 100 📦 STARTER PACK", "boxes_starter", 100, "business_box", 1),

        # GAMER'S CASE
        ("🎮 Начинающий геймер", "Открыть 10 🎮 GAMER'S CASE", "boxes_gamer", 10, "gamer_case", 1),
        ("🎮 Опытный геймер", "Открыть 25 🎮 GAMER'S CASE", "boxes_gamer", 25, "gamer_case", 5),
        ("🎮 Про-геймер", "Открыть 50 🎮 GAMER'S CASE", "boxes_gamer", 50, "business_box", 3),
        ("🎮 Геймер-легенда", "Открыть 100 🎮 GAMER'S CASE", "boxes_gamer", 100, "champion_chest", 1),

        # BUSINESS BOX
        ("💼 Начинающий бизнесмен", "Открыть 10 💼 BUSINESS BOX", "boxes_business", 10, "business_box", 1),
        ("💼 Деловой партнер", "Открыть 25 💼 BUSINESS BOX", "boxes_business", 25, "business_box", 5),
        ("💼 Бизнес-магнат", "Открыть 50 💼 BUSINESS BOX", "boxes_business", 50, "champion_chest", 3),
        ("💼 Король бизнеса", "Открыть 100 💼 BUSINESS BOX", "boxes_business", 100, "pro_gear", 1),

        # CHAMPION CHEST
        ("🏆 Начинающий чемпион", "Открыть 10 🏆 CHAMPION CHEST", "boxes_champion", 10, "champion_chest", 1),
        ("🏆 Чемпион-коллекционер", "Открыть 25 🏆 CHAMPION CHEST", "boxes_champion", 25, "champion_chest", 5),
        ("🏆 Великий чемпион", "Открыть 50 🏆 CHAMPION CHEST", "boxes_champion", 50, "pro_gear", 3),
        ("🏆 Легендарный чемпион", "Открыть 100 🏆 CHAMPION CHEST", "boxes_champion", 100, "legend_vault", 5),

        # PRO GEAR
        ("🧳 Начинающий профи", "Открыть 10 🧳 PRO GEAR", "boxes_pro", 10, "pro_gear", 1),
        ("🧳 Опытный профи", "Открыть 25 🧳 PRO GEAR", "boxes_pro", 25, "pro_gear", 5),
        ("🧳 Мастер профессионал", "Открыть 50 🧳 PRO GEAR", "boxes_pro", 50, "legend_vault", 1),
        ("🧳 Легенда профи", "Открыть 100 🧳 PRO GEAR", "boxes_pro", 100, "legend_vault", 3),

        # LEGEND'S VAULT
        ("👑 Начинающая легенда", "Открыть 10 👑 LEGEND'S VAULT", "boxes_legend", 10, "legend_vault", 1),
        ("👑 Легенда-коллекционер", "Открыть 25 👑 LEGEND'S VAULT", "boxes_legend", 25, "legend_vault", 5),
        ("👑 Великая легенда", "Открыть 50 👑 LEGEND'S VAULT", "boxes_legend", 50, "vip_mystery", 1),
        ("👑 Бессмертная легенда", "Открыть 100 👑 LEGEND'S VAULT", "boxes_legend", 100, "vip_mystery", 3),

        # VIP MYSTERY BOX
        ("🌟 VIP-новичок", "Открыть 10 🌟 VIP MYSTERY BOX", "boxes_vip", 10, "vip_mystery", 1),
        ("🌟 VIP-коллекционер", "Открыть 25 🌟 VIP MYSTERY BOX", "boxes_vip", 25, "vip_mystery", 3),
        ("🌟 VIP-магнат", "Открыть 50 🌟 VIP MYSTERY BOX", "boxes_vip", 50, "vip_mystery", 5),
        ("🌟 VIP-император", "Открыть 100 🌟 VIP MYSTERY BOX", "boxes_vip", 100, "vip_mystery", 10),
    ]

    for ach in achievements:
        cursor.execute('''
        INSERT INTO achievements (name, description, category, target_value, reward_type, reward_value)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', ach)

    print(f'✓ Added {len(achievements)} box achievements')
else:
    print(f'\nBox achievements already exist ({count} found), skipping')

conn.commit()
conn.close()

print('\n✓ Database migration completed successfully!')
print('Теперь перезапусти бота.')
