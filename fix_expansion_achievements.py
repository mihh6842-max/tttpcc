"""
Скрипт для миграции достижений экспансии для существующих пользователей
"""
import aiosqlite
import asyncio

DB_PATH = '2pcclub.db'

async def fix_expansion_achievements():
    """Синхронизировать expansion_level из stats в user_achievement_stats"""
    async with aiosqlite.connect(DB_PATH) as conn:
        # Получаем всех пользователей с expansion_level > 0
        cursor = await conn.execute('SELECT userid, expansion_level FROM stats WHERE expansion_level > 0')
        users = await cursor.fetchall()

        print(f"Найдено {len(users)} пользователей с экспансией")

        for user_id, expansion_level in users:
            # Проверяем/создаем запись в user_achievement_stats
            cursor = await conn.execute('SELECT max_expansion_level FROM user_achievement_stats WHERE user_id = ?', (user_id,))
            result = await cursor.fetchone()

            if not result:
                # Создаем новую запись
                await conn.execute('''
                    INSERT INTO user_achievement_stats (user_id, max_expansion_level)
                    VALUES (?, ?)
                ''', (user_id, expansion_level))
                print(f"Пользователь {user_id}: создана запись с expansion_level={expansion_level}")
            else:
                current_max = result[0] if result[0] else 0
                if expansion_level > current_max:
                    # Обновляем
                    await conn.execute('UPDATE user_achievement_stats SET max_expansion_level = ? WHERE user_id = ?', (expansion_level, user_id))
                    print(f"Пользователь {user_id}: обновлен expansion_level {current_max} -> {expansion_level}")

            # Получаем достижения категории expansion
            cursor = await conn.execute('SELECT id, target_value FROM achievements WHERE category = ?', ('expansion',))
            achievements = await cursor.fetchall()

            for ach_id, target in achievements:
                # Создаем/обновляем запись в user_achievements
                await conn.execute('''
                    INSERT OR IGNORE INTO user_achievements (user_id, achievement_id, current_value, completed)
                    VALUES (?, ?, 0, 0)
                ''', (user_id, ach_id))

                # Обновляем прогресс
                completed = 1 if expansion_level >= target else 0
                await conn.execute('''
                    UPDATE user_achievements
                    SET current_value = ?, completed = ?
                    WHERE user_id = ? AND achievement_id = ?
                ''', (expansion_level, completed, user_id, ach_id))

                if completed:
                    print(f"  Достижение {ach_id} (target={target}): выполнено")

        await conn.commit()
        print("\nМиграция завершена!")

if __name__ == '__main__':
    asyncio.run(fix_expansion_achievements())
