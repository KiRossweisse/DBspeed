import redis
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

TOTAL_RUNS = 30  # Запускаем 30 раз для замера среднего времени
SCANNED_LIMIT = 500_000  # Останавливаемся после проверки 250k ключей

total_exec_time = 0

for _ in range(TOTAL_RUNS):
    scanned_keys = 0  # Количество просмотренных ключей
    cursor = 0

    start_time = time.time()

    # Используем SCAN для обхода всех ключей с префиксом game:
    while True:
        cursor, keys = r.scan(cursor, match="game:*", count=5000)  # batch 5000
        scanned_keys += len(keys)

        if not keys:
            continue

        # Используем pipeline для ускоренного получения данных с hgetall
        with r.pipeline() as pipe:
            for key in keys:
                pipe.hgetall(key)
            results = pipe.execute()

        # Прерываем, если проверено 250 000 ключей
        if scanned_keys >= SCANNED_LIMIT:
            break

    end_time = time.time()
    exec_time = end_time - start_time
    total_exec_time += exec_time
    print(f"🔹 Запуск {_ + 1}: {exec_time:.3f} сек, просмотрено {scanned_keys} ключей")

average_exec_time = total_exec_time / TOTAL_RUNS
print(f"\n🚀 Среднее время выполнения: {average_exec_time:.3f} сек")
