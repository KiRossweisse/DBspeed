import pymysql
import time

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    database='game_reviews',
    local_infile=True
)

cursor = connection.cursor()

sql_query = """
SELECT g.title, g.genre, r.review_date
FROM games g
JOIN reviews r ON g.game_id = r.game_id
JOIN players p ON r.player_id = p.player_id
WHERE YEAR(r.review_date) = 2022
AND p.nickname = "April Moore"
GROUP BY g.title, g.genre, r.review_date
HAVING COUNT(*) > (
    SELECT AVG(reviews_per_game)
    FROM (
        SELECT COUNT(*) AS reviews_per_game
        FROM reviews
        WHERE YEAR(review_date) = 2022
        GROUP BY game_id
    ) AS subquery
)
LIMIT 1000000;
"""

start_time = time.time()
cursor.execute(sql_query)

end_time = time.time()

total_time = end_time - start_time

print(f"Total execution time: {total_time:.4f} seconds")

results = cursor.fetchall()
for doc in results:
    pass

cursor.close()
connection.close()
