from flask import Flask, request, jsonify
from pymongo import MongoClient
import pandas as pd
import redis
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import os

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['titanic_db']
collection = db['passengers']

# Redis connection
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# ThreadPool for handling requests
executor = ThreadPoolExecutor(max_workers=8)

def insert_data(data):
    collection.insert_many(data)

def update_record_in_db(passenger_id, update_data):
    result = collection.update_one({"PassengerId": int(passenger_id)}, {"$set": update_data})
    return result

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.float64) and np.isnan(obj):
            return None
        return super(NumpyEncoder, self).default(obj)

def custom_decoder(dct):
    for key, value in dct.items():
        if value == "NaN":
            dct[key] = None
    return dct

# Endpoint to upload the Excel file
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
        
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    if file and file.filename.endswith('.xlsx'):
        df = pd.read_excel(file)
        data = df.to_dict(orient='records')
        executor.submit(insert_data, data)
        return jsonify({"message": "File uploaded and data insertion started"}), 201
    else:
        return jsonify({"error": "Invalid file format"}), 400

# Endpoint to fetch a record based on passenger_id
@app.route('/get_record', methods=['GET'])
def get_record():
    passenger_id = request.args.get('passenger_id')
    if not passenger_id:
        return jsonify({"error": "Passenger ID is required"}), 400
    
    # Check Redis cache first
    cached_record = redis_client.get(passenger_id)
    if cached_record:
        return jsonify(json.loads(cached_record, object_hook=custom_decoder)), 200

    # If not in cache, query MongoDB
    record = collection.find_one({"PassengerId": int(passenger_id)})
    if record:
        record.pop('_id', None)  # Remove the MongoDB ID from the response
        # Cache the result in Redis using the custom encoder
        redis_client.set(passenger_id, json.dumps(record, cls=NumpyEncoder))
        return jsonify(record), 200
    else:
        return jsonify({"error": "Record not found"}), 404

# Endpoint to update data based on passenger_id
@app.route('/update_record', methods=['PUT'])
def update_record():
    passenger_id = request.json.get('passenger_id')
    passenger_id = int(passenger_id)
    update_data = request.json.get('update_data')
    
    if not passenger_id or not update_data:
        return jsonify({"error": "Passenger ID and update data are required"}), 400
    
    future = executor.submit(update_record_in_db, passenger_id, update_data)
    result = future.result()
    
    if result.matched_count > 0:
        redis_client.delete(passenger_id)  # Invalidate cache
        return jsonify({"message": "Record updated"}), 200
    else:
        return jsonify({"error": "Record not found"}), 404
@app.route('/survived', methods=['POST','GET'])
def count_survived_passengers():
    # Get JSON data from the request
    data = request.json
    gender = data.get('gender')
    
    if gender not in ['male', 'female']:
        return jsonify({"error": "Invalid gender specified. Use 'male' or 'female'."}), 400
    
    # MongoDB query to count passengers who survived, are under 45, and match the specified gender
    count = collection.count_documents({
        "Sex": gender,
        "Survived": 1,  # Assuming 1 means survived
        "Age": {"$lt": 45}
    })
    
    return jsonify({"count": count}), 200

if __name__ == '__main__':
    app.run(debug=True)
