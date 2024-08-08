from pymongo import MongoClient
import random
import string
import time

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client.mydatabase
users_collection = db.users

# Function to generate random user data
def generate_user(user_id):
    return {
        'id': user_id,
        'name': ''.join(random.choices(string.ascii_letters, k=10)),
        'age': random.randint(18, 80),
        'email': ''.join(random.choices(string.ascii_letters, k=10)) + '@example.com'
    }

# Insert 100,000 users
bulk_users = [generate_user(i) for i in range(1, 100001)]

# Start timing
start_time = time.time()

users_collection.insert_many(bulk_users)

# End timing
end_time = time.time()

# Calculate total time taken
total_time = end_time - start_time

# Calculate average time per user
average_time_per_user = total_time / 100000

print(f"Inserted 100,000 users successfully in {total_time:.2f} seconds")
print(f"Average time per user: {average_time_per_user:.6f} seconds")
