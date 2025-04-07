import argparse
import json
import os
from pymongo import MongoClient

def load_config(path):
    with open(path, 'r') as f:
        return json.load(f)

def insert_documents(mongo_uri, db_name, collection_name, documents):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(documents)

def generate_patient_data(start, count, min_compositions):
    all_docs = []
    for patient_id in range(start, start + count):
        for _ in range(min_compositions):
            doc = {
                "patient_id": patient_id,
                "data": "synthetic_example_data"
            }
            all_docs.append(doc)
    return all_docs

def main():
    parser = argparse.ArgumentParser(description="Hybrid Insert Script for MongoDB Atlas")
    parser.add_argument('--start_patient', type=int, required=True, help='Start index for patient generation')
    parser.add_argument('--num_patients', type=int, required=True, help='Number of patients to generate')
    parser.add_argument('--mongo_uri', type=str, default=os.getenv("MONGO_URI"), help='MongoDB connection URI')
    parser.add_argument('--config_file', type=str, default='config.json', help='Path to configuration file')

    args = parser.parse_args()

    config = load_config(args.config_file)

    min_compositions = config['synthetic_generation']['min_compositions']
    db_name = config['source_database_name']
    target_collection = config['target_collection']

    # Log info
    print(f"Generating {args.num_patients} patients starting from {args.start_patient}")
    print(f"Inserting into DB: {db_name}, Collection: {target_collection}")

    docs = generate_patient_data(args.start_patient, args.num_patients, min_compositions)

    print(f"Generated {len(docs)} documents. Starting insert...")

    insert_documents(args.mongo_uri, db_name, target_collection, docs)

    print("Insert complete.")

if __name__ == "__main__":
    main()
