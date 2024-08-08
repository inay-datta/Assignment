import pymongo
import threading
from queue import Queue
import logging
import time

# MongoDB setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["testdb"]
collection = db["testcollection"]

# Number of documents to insert
num_docs = 100000

# Set up logging
logging.basicConfig(filename='insert_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Function to insert documents
def insert_documents(queue, counts, thread_name):
    count = 0
    while not queue.empty():
        document = queue.get()
        start_time = time.time()
        collection.insert_one(document)
        end_time = time.time()
        time_taken = end_time - start_time
        logging.info(f"Thread {thread_name} inserted document {document['index']} in {time_taken:.5f} seconds")
        count += 1
        queue.task_done()
    counts[thread_name] = count

# Prepare data
data_queue = Queue()
for i in range(num_docs):
    data_queue.put({"index": i, "value": f"value_{i}"})

# Number of threads
num_threads = 10

# Dictionary to store counts for each thread
counts = {}

# Create and start threads
threads = []
start_time = time.time()  # Start time for the entire operation
for i in range(num_threads):
    thread_name = f"Thread-{i+1}"
    thread = threading.Thread(target=insert_documents, args=(data_queue, counts, thread_name))
    thread.start()
    threads.append(thread)

# Wait for all threads to finish
for thread in threads:
    thread.join()

end_time = time.time()  # End time for the entire operation
total_time = end_time - start_time

# Log the number of documents inserted by each thread
for thread_name, count in counts.items():
    logging.info(f"{thread_name} inserted {count} documents")

print(f"Inserted {num_docs} documents into MongoDB in {total_time:.2f} seconds.")
