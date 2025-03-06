import pymongo
import time
import statistics
import math

# Creating MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Reviews"]
collection = db["games"]

# List for every runtime
execution_times = []

# Function for cheking the runtime and query
def execute_query():
    start_time = time.time()
    # The query
    pipeline = [
        {
            "$limit": 1000000
        },
        {
            "$lookup": {
                "from": "games",
                "localField": "game_id",
                "foreignField": "game_id",
                "as": "game_info"
            }
        },
        {
            "$unwind": "$game_info"
        },
        {
            "$match": {
                "game_info.genre": "Shooter",
                "mark": 5,
                "review_date": {
                    "$regex": "^2020"
                }
            }
        },
        {
            "$sort": {
                "review_date": -1
            }
        }
    ]

    # Query execution
    result = collection.aggregate(pipeline)
    for doc in result:
        pass
    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time

# Function usage
for _ in range(30):
    execution_time = execute_query()
    execution_times.append(execution_time)

# Mean
mean_execution_time = statistics.mean(execution_times)

# Confidence interval
confidence_interval = 1.96 * statistics.stdev(execution_times) / math.sqrt(len(execution_times))
lower_bound = mean_execution_time - confidence_interval
upper_bound = mean_execution_time + confidence_interval

print(f"Average runtime: {mean_execution_time:.4f} secs")
print(f"95% confidence interval: [{lower_bound:.4f}, {upper_bound:.4f}] secs")
