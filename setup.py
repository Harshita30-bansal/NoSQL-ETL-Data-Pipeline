"""
Setup Script - Initialize database and project structure
Run this once before executing pipelines
"""

import os
import sys
from config import DB_CONFIG, DEFAULT_DB
import pymysql


def create_postgresql_database():
    """Create PostgreSQL database and tables"""
    print("INITIALIZING POSTGRESQL DATABASE")
    print("\n\n\n")

    try:
        import psycopg2
        
        config = DB_CONFIG['postgresql']
        
        # 1. Connect to default postgres database to create the new DB
        connection = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database='postgres'
        )
        
        connection.autocommit = True
        cursor = connection.cursor()
        
        db_name = config['database']
        print(f"Creating database: {db_name}...")
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        cursor.execute(f"CREATE DATABASE {db_name}")
        print(f"✓ Database created: {db_name}")
        
        cursor.close()
        connection.close()
        
        # 2. Connect to the newly created database to initialize schema
        connection = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=db_name
        )
        
        cursor = connection.cursor()
        
        # Read the schema file
        with open('db/schema.sql', 'r') as f:
            schema = f.read()
        
        # Clean and adapt SQL for PostgreSQL 
        # (These ensures compatibility if your schema has MySQL-style syntax)
        schema = schema.replace('AUTO_INCREMENT', 'SERIAL')
        schema = schema.replace('INT PRIMARY KEY AUTO_INCREMENT', 'SERIAL PRIMARY KEY')
        schema = schema.replace('JSON ', 'jsonb ') # Project uses JSONB for results
        
        print("Creating tables and indexes...")
        
        # CHANGE: Execute the entire schema as one block.
        # This preserves Foreign Key relationships and table dependencies.
        cursor.execute(schema)
        
        print("Tables and indexes created successfully")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        print("\nPostgreSQL initialization completed!\n")
        return True
        
    except Exception as e:
        print(f"\nPostgreSQL Error: {e}\n")
        return False


def create_project_directories():
    """Create necessary project directories"""
    
    print("CREATING PROJECT DIRECTORIES")
    print("\n\n\n")

    directories = [
        'data',
        'db',
        'docs',
        'hive_scripts',
        'logs',
        'mapreduce',
        'pig_scripts',
        'pipelines',
        'reports',
        'results',
        'src',
        'tests'
    ]
    
    for dir_name in directories:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"✓ Created: {dir_name}/")
        else:
            print(f"  Exists: {dir_name}/")

    print("\n All directories are ready!\n")


def verify_data_files():
    """Verify that data files exist"""
    print("VERIFYING DATA FILES")
    print("\n\n\n")

    files_required = [
        'data/NASA_access_log_Jul95',
        'data/NASA_access_log_Aug95'
    ]
    
    all_exist = True
    for file_path in files_required:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f" Found: {file_path} ({size:,} bytes)")
        else:
            print(f" Missing: {file_path}")
            all_exist = False
    
    if not all_exist:
        print("\n Some data files are missing.")
        print("Please download NASA logs from:")
        print("  https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz")
        print("  https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz")
        print("  Then decompress and place in data/ directory\n")
    else:
        print("\n All data files are present!\n")

    return all_exist


def main():
    """Main setup routine"""
    print("ETL FRAMEWORK - SETUP & INITIALIZATION")
    print("\n\n\n")

    # Create directories
    create_project_directories()

    # Verify data files
    verify_data_files()

    
    create_postgresql_database()
    
    print("SETUP COMPLETED!")

    print("\nYou can now run the ETL framework with:")
    print("  python orchestrator.py\n")


if __name__ == "__main__":
    main()
