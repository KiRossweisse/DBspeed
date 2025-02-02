import redis
import time
from collections import defaultdict

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Настроим параметры запроса
TOTAL_RUNS = 1  # Запускаем один раз для теста
SCANNED_LIMIT = 250_000  # Останавливаемся после проверки 250k ключей

total_exec_time = 0
games_review_count = defaultdict(int)  # Счётчик отзывов по каждой игре
games_ratings_sum = defaultdict(int)  # Сумма оценок по каждой игре
games_ratings_avg = {}  # Среднее значение по каждой игре

for _ in range(TOTAL_RUNS):
    count = 0
    cursor = 0
    scanned_keys = 0  # Количество просмотренных ключей

    start_time = time.time()

    while True:
        cursor, review_keys = r.scan(cursor, match="reviews:*", count=5000)  # batch 5000
        scanned_keys += len(review_keys)

        if not review_keys:
            continue

        # Используем pipeline для ускоренного получения данных
        with r.pipeline() as pipe:
            for key in review_keys:
                pipe.hgetall(key)
            results = pipe.execute()

        for review_data in results:
            # Извлекаем данные из review
            game_id = review_data.get("game_id", "")
            player_id = review_data.get("player_id", "")
            mark = int(review_data.get("mark", 0))
            review_date = review_data.get("review_date", "")

            # Фильтруем только отзывы за 2023 год
            if review_date.startswith("2023") and mark > 4:
                # Получаем данные о игре
                game_key = f"game:{game_id}"
                game_data = r.hgetall(game_key)
                game_title = game_data.get("title", "Unknown Game")
                game_genre = game_data.get("genre", "Unknown Genre")

                # Получаем данные о игроке
                player_key = f"player:{player_id}"
                player_data = r.hgetall(player_key)
                player_nickname = player_data.get("nickname", "Unknown Player")

                # Считаем количество отзывов и сумму оценок
                games_review_count[game_id] += 1
                games_ratings_sum[game_id] += mark

                # Печатаем данные отзыва
                #print(f"Review ID: {key}, Game: {game_title}, Genre: {game_genre}, Reviewer: {player_nickname}, Mark: {mark}, Date: {review_date}")

        # Останавливаемся, если проверили 250 000 ключей
        if scanned_keys >= SCANNED_LIMIT or cursor == 0:
            break

    # Вычисляем среднюю оценку по каждой игре
    for game_id, total_reviews in games_review_count.items():
        if total_reviews >= 5:  # Игры, имеющие хотя бы 5 отзывов
            games_ratings_avg[game_id] = games_ratings_sum[game_id] / total_reviews

    end_time = time.time()
    exec_time = end_time - start_time
    total_exec_time += exec_time
    print(f"🔹 Запуск {_ + 1}: {exec_time:.3f} сек, просмотрено {scanned_keys} ключей")

# Печатаем среднее значение оценок для игр с хотя бы 5 отзывами
print("\n🚀 Средние оценки по играм с более чем 5 отзывами в 2023 году:")
for game_id, avg_rating in games_ratings_avg.items():
    game_key = f"game:{game_id}"
    game_data = r.hgetall(game_key)
    game_title = game_data.get("title", "Unknown Game")
    print(f"Game: {game_title}, Average Rating: {avg_rating:.2f}")

average_exec_time = total_exec_time / TOTAL_RUNS
print(f"\n🚀 Среднее время выполнения: {average_exec_time:.3f} сек")
