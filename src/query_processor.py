"""
Query Processing Engine - implements the 3 mandatory queries
These functions are used by all pipelines to ensure consistency
"""

from collections import defaultdict
from config import ERROR_STATUS_MIN, ERROR_STATUS_MAX


class QueryProcessor:
    """Process parsed log records and compute aggregate results"""

    @staticmethod
    def query_1_daily_traffic_summary(records):
        """
        Query 1: Daily Traffic Summary
        For each (log_date, status_code), compute total requests and bytes
        
        Output: list of dicts with keys:
        - log_date
        - status_code
        - request_count
        - total_bytes
        """
        traffic_summary = defaultdict(lambda: {'request_count': 0, 'total_bytes': 0})

        for record in records:
            if record is None:
                continue
            
            key = (record['log_date'], record['status_code'])
            traffic_summary[key]['request_count'] += 1
            traffic_summary[key]['total_bytes'] += record['bytes_transferred']

        results = []
        for (log_date, status_code), aggregates in traffic_summary.items():
            results.append({
                'log_date': log_date,
                'status_code': status_code,
                'request_count': aggregates['request_count'],
                'total_bytes': aggregates['total_bytes']
            })

        return sorted(results, key=lambda x: (x['log_date'], x['status_code']))

    @staticmethod
    def query_2_top_requested_resources(records):
        """
        Query 2: Top Requested Resources
        Compute top 20 resources by request count.
        For each: total requests, total bytes, distinct hosts
        
        Output: list of top 20 dicts with keys:
        - resource_path
        - request_count
        - total_bytes
        - distinct_host_count
        """
        resources = defaultdict(lambda: {
            'request_count': 0,
            'total_bytes': 0,
            'hosts': set()
        })

        for record in records:
            if record is None or record['resource_path'] is None:
                continue
            
            resource = record['resource_path']
            resources[resource]['request_count'] += 1
            resources[resource]['total_bytes'] += record['bytes_transferred']
            resources[resource]['hosts'].add(record['host'])

        results = []
        for resource_path, data in resources.items():
            results.append({
                'resource_path': resource_path,
                'request_count': data['request_count'],
                'total_bytes': data['total_bytes'],
                'distinct_host_count': len(data['hosts'])
            })

        # Sort by request count (descending) and return top 20
        results = sorted(results, key=lambda x: x['request_count'], reverse=True)[:20]
        return results

    @staticmethod
    def query_3_hourly_error_analysis(records):
        """
        Query 3: Hourly Error Analysis
        For each (log_date, log_hour), compute error stats (status 400-599)
        
        Output: list of dicts with keys:
        - log_date
        - log_hour
        - error_request_count
        - total_request_count
        - error_rate
        - distinct_error_hosts
        """
        hourly_stats = defaultdict(lambda: {
            'error_count': 0,
            'total_count': 0,
            'error_hosts': set()
        })

        for record in records:
            if record is None:
                continue
            
            key = (record['log_date'], record['log_hour'])
            hourly_stats[key]['total_count'] += 1
            
            status = record['status_code']
            if ERROR_STATUS_MIN <= status <= ERROR_STATUS_MAX:
                hourly_stats[key]['error_count'] += 1
                hourly_stats[key]['error_hosts'].add(record['host'])

        results = []
        for (log_date, log_hour), data in hourly_stats.items():
            error_rate = (data['error_count'] / data['total_count']) if data['total_count'] > 0 else 0.0
            results.append({
                'log_date': log_date,
                'log_hour': log_hour,
                'error_request_count': data['error_count'],
                'total_request_count': data['total_count'],
                'error_rate': round(error_rate, 4),
                'distinct_error_hosts': len(data['error_hosts'])
            })

        return sorted(results, key=lambda x: (x['log_date'], x['log_hour']))

    @staticmethod
    def process_all_queries(records):
        """
        Process all three queries on the same record set
        Returns dict with results for each query
        """
        return {
            'query_1': QueryProcessor.query_1_daily_traffic_summary(records),
            'query_2': QueryProcessor.query_2_top_requested_resources(records),
            'query_3': QueryProcessor.query_3_hourly_error_analysis(records)
        }
