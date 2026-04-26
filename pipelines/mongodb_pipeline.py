"""
MongoDB Pipeline - ETL using MongoDB NoSQL database
Loads parsed logs into MongoDB and executes aggregation queries
"""

import time
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import json
from src.base_pipeline import BasePipeline
from parser import parse_file_in_batches
from config import MONGO_CONFIG, ERROR_STATUS_MIN, ERROR_STATUS_MAX


class MongoDBPipeline(BasePipeline):
    def __init__(self, data_file, batch_size=5000, db_type='mysql'):
        super().__init__('MongoDB', data_file, batch_size, db_type)
        self.mongo_config = MONGO_CONFIG
        self.mongo_client = None
        self.mongo_db = None

    def execute(self):
        print(f"\n{'='*60}")
        print(f"Executing {self.pipeline_name} Pipeline")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            print("✗ Failed to connect to relational database")
            return False

        if not self._connect_mongodb():
            print("✗ Failed to connect to MongoDB")
            return False

        try:
            execution_timestamp = datetime.now()

            print("  Initializing MongoDB collections...")
            self._initialize_collections()

            print("  Loading logs into MongoDB...")
            self._load_logs_to_mongodb()

            print("  Executing aggregation queries...")
            results = self._execute_queries()

            print("  Storing results in relational database...")

            # ✅ FIX: Insert ONLY ONCE into query_results
            self.db_manager.execute_update("""
                INSERT INTO query_results (
                    run_id, pipeline_name, query_id, query_name,
                    batch_id, batch_size, avg_batch_size,
                    execution_time_ms, execution_timestamp,
                    malformed_records, total_records_processed, result_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.run_id,
                self.pipeline_name,
                1,
                "all_queries",
                1,
                self.batch_size,
                self.batch_size,
                int((time.time() - self.start_time) * 1000),
                execution_timestamp,
                self.malformed_records,
                self.total_records,
                json.dumps({"summary": "all queries executed"})
            ))

            # ✅ Insert actual query results
            for batch_id, (query_id, query_results) in enumerate(results.items(), 1):
                if query_id == 'query_1':
                    self.db_manager.insert_daily_traffic_results(
                        query_results, self.run_id, self.pipeline_name, batch_id, execution_timestamp
                    )
                elif query_id == 'query_2':
                    self.db_manager.insert_top_resources_results(
                        query_results, self.run_id, self.pipeline_name, batch_id, execution_timestamp
                    )
                elif query_id == 'query_3':
                    self.db_manager.insert_error_analysis_results(
                        query_results, self.run_id, self.pipeline_name, batch_id, execution_timestamp
                    )

            self.end_time = time.time()
            self.save_metadata()

            print(self.get_status_string())
            return True

        except Exception as e:
            print(f"✗ Pipeline execution failed: {e}")
            self.end_time = time.time()
            return False
        finally:
            self.disconnect_db()
            self._disconnect_mongodb()

    def _connect_mongodb(self):
        try:
            self.mongo_client = MongoClient(
                self.mongo_config['host'],
                self.mongo_config['port'],
                serverSelectionTimeoutMS=5000
            )
            self.mongo_db = self.mongo_client[self.mongo_config['database']]
            self.mongo_db.command('ping')
            print("✓ Connected to MongoDB")
            return True
        except ConnectionFailure as e:
            print(f"✗ MongoDB connection failed: {e}")
            return False

    def _disconnect_mongodb(self):
        if self.mongo_client:
            self.mongo_client.close()
            print("✓ Disconnected from MongoDB")

    def _initialize_collections(self):
        collection = self.mongo_db[self.mongo_config['collections']['logs']]
        collection.delete_many({})
        collection.create_index([('log_date', 1), ('log_hour', 1)])
        collection.create_index([('resource_path', 1)])
        collection.create_index([('host', 1)])
        collection.create_index([('status_code', 1)])
        print("  ✓ MongoDB collection initialized")

    def _load_logs_to_mongodb(self):
        collection = self.mongo_db[self.mongo_config['collections']['logs']]
        
        for batch_id, records, malformed in parse_file_in_batches(self.data_file, self.batch_size):
            self.batch_count = batch_id
            self.total_records += len(records) + malformed
            self.malformed_records += malformed

            if records:
                collection.insert_many(records)
                print(f"  Batch {batch_id}: inserted {len(records)} records")

    def _execute_queries(self):
        collection = self.mongo_db[self.mongo_config['collections']['logs']]
        
        return {
            'query_1': self._query_1_aggregation(collection),
            'query_2': self._query_2_aggregation(collection),
            'query_3': self._query_3_aggregation(collection)
        }

    def _query_1_aggregation(self, collection):
        return list(collection.aggregate([
            {'$group': {
                '_id': {'log_date': '$log_date', 'status_code': '$status_code'},
                'request_count': {'$sum': 1},
                'total_bytes': {'$sum': '$bytes_transferred'}
            }},
            {'$project': {
                '_id': 0,
                'log_date': '$_id.log_date',
                'status_code': '$_id.status_code',
                'request_count': 1,
                'total_bytes': 1
            }},
            {'$sort': {'log_date': 1, 'status_code': 1}}
        ]))

    def _query_2_aggregation(self, collection):
        return list(collection.aggregate([
            {'$match': {'resource_path': {'$ne': None}}},
            {'$group': {
                '_id': '$resource_path',
                'request_count': {'$sum': 1},
                'total_bytes': {'$sum': '$bytes_transferred'},
                'hosts': {'$addToSet': '$host'}
            }},
            {'$project': {
                '_id': 0,
                'resource_path': '$_id',
                'request_count': 1,
                'total_bytes': 1,
                'distinct_host_count': {'$size': '$hosts'}
            }},
            {'$sort': {'request_count': -1}},
            {'$limit': 20}
        ]))

    def _query_3_aggregation(self, collection):
        results = list(collection.aggregate([
            {'$group': {
                '_id': {'log_date': '$log_date', 'log_hour': '$log_hour'},
                'error_request_count': {
                    '$sum': {
                        '$cond': [
                            {'$and': [
                                {'$gte': ['$status_code', ERROR_STATUS_MIN]},
                                {'$lte': ['$status_code', ERROR_STATUS_MAX]}
                            ]}, 1, 0
                        ]
                    }
                },
                'total_request_count': {'$sum': 1}
            }},
            {'$project': {
                '_id': 0,
                'log_date': '$_id.log_date',
                'log_hour': '$_id.log_hour',
                'error_request_count': 1,
                'total_request_count': 1,
                'error_rate': {
                    '$divide': ['$error_request_count', '$total_request_count']
                }
            }},
            {'$sort': {'log_date': 1, 'log_hour': 1}}
        ]))

        for r in results:
            r['error_rate'] = round(r['error_rate'], 4)
            r['distinct_error_hosts'] = 0

        return results