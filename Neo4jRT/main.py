from neo4j import GraphDatabase
import time
import statistics
import math
# Connect to the Neo4j database
uri = "bolt://localhost:7687"
username = "neo4j"
password = "Nekron12369"

# Define the query
query = """
MATCH (g:Game)
WHERE substring(toString(g.release_date), 0, 4) = '2022' AND g.genre = 'Shooter'
MATCH (r:Reviews)-[:REVIEWS]->(g)
MATCH (p:Players)-[:WROTE]->(r)
RETURN g.title AS game_title,
g.genre AS game_genre,
p.nickname AS player_nickname,
r.mark AS review_mark,
r.review_date AS review_date
LIMIT 1000000
"""
# Function to execute the query and measure time
def execute_query_and_measure_time(query):
    start_time = time.time()

    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            result = session.run(query)
            for record in result:
                # Do something with the result if needed
                pass

    end_time = time.time()
    return end_time - start_time

# Execute the query and measure time
execution_times=[]
for _ in range(30):
    execution_time = execute_query_and_measure_time(query)
    execution_times.append(execution_time)
# Mean
mean_execution_time = statistics.mean(execution_times)

# Confidence interval
confidence_interval = 1.96 * statistics.stdev(execution_times) / math.sqrt(len(execution_times))
lower_bound = mean_execution_time - confidence_interval
upper_bound = mean_execution_time + confidence_interval

print(f"Average runtime: {mean_execution_time:.3f} secs")
print(f"95% confidence interval: [{lower_bound:.3f}, {upper_bound:.3f}] secs")
