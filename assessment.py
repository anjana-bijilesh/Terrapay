import argparse
from datetime import datetime
import json
import mysql.connector
import logging as log

# Database connection parameters
DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASSWORD = '12345'
DB_NAME = 'new'
DB_PORT = '3306'


def connect_to_database():
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )
    except Exception as e:
        log.error(e)
        return None


def run_query(query_name, start_date, end_date, category):
    conn = None
    curser = None
    queries = {
        'demand': "SELECT * FROM data_t WHERE InvoiceDate BETWEEN %s AND %s AND Description LIKE %s"
        # Add more queries here as needed
    }

    if query_name not in queries:
        print(f"Error: Query '{query_name}' not found.")
        return None

    sql_query = queries[query_name]
    conn = connect_to_database()
    if conn:
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql_query, (start_date, end_date, f'%{category}%'))
            results = cursor.fetchall()
            print(results)
            return results
        except Exception as e:
            log.error(e)
            return None
        finally:
            cursor.close()
            conn.close()
    else:
        return None


def parse_datetime(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise argparse.ArgumentTypeError("Invalid date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS.")


def parse_arguments():
    parser = argparse.ArgumentParser(description='Run SQL queries on e-commerce data')
    parser.add_argument('query_name', type=str, help='Name of the SQL query to run')
    parser.add_argument('start_date', type=parse_datetime, help='Start date for the time window (YYYY-MM-DD)')
    parser.add_argument('end_date', type=parse_datetime, help='End date for the time window (YYYY-MM-DD)')
    parser.add_argument('category', type=str, help='Substring of product description')
    return parser.parse_args()


# python terrapay_test.py demand '2010-01-01 11:33:00'  '2011-01-10 11:33:00' T-Shirt
# python terrapay_test.py demand 2010-01-01  2011-01-10 T-Shirt

def main():
    try:
        args = parse_arguments()
        run_query(args.query_name, args.start_date, args.end_date, args.category)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
