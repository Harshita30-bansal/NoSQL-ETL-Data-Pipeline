"""
Setup Script - Initialize database and project structure
Run this once before executing pipelines
"""

import os
import sys
from config import DB_CONFIG, DEFAULT_DB
import pymysql


def create_mysql_database():
    """Create MySQL database and tables"""
    print("\n" + "="*70)
    print("INITIALIZING MYSQL DATABASE")
    print("="*70 + "\n")

    config = DB_CONFIG['mysql']
    
    try:
        # Connect to MySQL server
        connection = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            charset='utf8mb4'
        )
        
        cursor = connection.cursor()
        
        # Create database
        db_name = config['database']
        print(f"Creating database: {db_name}...")
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        cursor.execute(f"CREATE DATABASE {db_name}")
        cursor.execute(f"USE {db_name}")
        print(f"✓ Database created: {db_name}")
        
        # Read schema
        with open('db/schema.sql', 'r') as f:
            schema = f.read()
        
        # Execute schema
        print("Creating tables...")
        statements = schema.split(';')
        for statement in statements:
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                cursor.execute(statement)
        
        print("✓ Tables created successfully")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        print("\n✓ MySQL initialization completed!\n")
        return True
        
    except pymysql.Error as e:
        print(f"\n✗ MySQL Error: {e}\n")
        return False
    except Exception as e:
        print(f"\n✗ Error: {e}\n")
        return False


def create_postgresql_database():
    """Create PostgreSQL database and tables"""
    print("\n" + "="*70)
    print("INITIALIZING POSTGRESQL DATABASE")
    print("="*70 + "\n")

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
        
        print("✓ Tables and indexes created successfully")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        print("\n✓ PostgreSQL initialization completed!\n")
        return True
        
    except Exception as e:
        print(f"\n✗ PostgreSQL Error: {e}\n")
        return False


def create_project_directories():
    """Create necessary project directories"""
    print("\n" + "="*70)
    print("CREATING PROJECT DIRECTORIES")
    print("="*70 + "\n")

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

    print("\n✓ All directories are ready!\n")


def verify_data_files():
    """Verify that data files exist"""
    print("\n" + "="*70)
    print("VERIFYING DATA FILES")
    print("="*70 + "\n")

    files_required = [
        'data/NASA_access_log_Jul95',
        'data/NASA_access_log_Aug95'
    ]
    
    all_exist = True
    for file_path in files_required:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"✓ Found: {file_path} ({size:,} bytes)")
        else:
            print(f"✗ Missing: {file_path}")
            all_exist = False
    
    if not all_exist:
        print("\n⚠ Some data files are missing.")
        print("Please download NASA logs from:")
        print("  https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz")
        print("  https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz")
        print("  Then decompress and place in data/ directory\n")
    else:
        print("\n✓ All data files are present!\n")

    return all_exist


def main():
    """Main setup routine"""
    print("\n" + "="*70)
    print("ETL FRAMEWORK - SETUP & INITIALIZATION")
    print("="*70)

    # Create directories
    create_project_directories()

    # Verify data files
    verify_data_files()

    # Initialize database
    print("Select database to initialize:")
    print("  1. MySQL")
    print("  2. PostgreSQL")
    print("  0. Skip database initialization")
    
    choice = input("\nEnter choice (0-2): ").strip()
    
    if choice == '1':
        if not create_mysql_database():
            print("✗ MySQL initialization failed")
            sys.exit(1)
    elif choice == '2':
        if not create_postgresql_database():
            print("✗ PostgreSQL initialization failed")
            sys.exit(1)
    elif choice != '0':
        print("✗ Invalid choice")
        sys.exit(1)

    print("="*70)
    print("✓ SETUP COMPLETED!")
    print("="*70)
    print("\nYou can now run the ETL framework with:")
    print("  python orchestrator.py\n")


if __name__ == "__main__":
    main()
