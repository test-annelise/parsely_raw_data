from __future__ import absolute_import, print_function

import logging
import pprint

from googleapiclient.discovery import build as google_build
from oauth2client.client import GoogleCredentials
from six import iteritems

from . import utils
from .s3 import events_s3


__license__ = """
Copyright 2016 Parsely, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

log = logging.getLogger(__name__)


def event_to_bigquery(event):
    """Convert a Event instance to a BigQuery-formatted dict

    Compatible with the schema implemented by `create_bigquery_table`

    :param event: The Event instance to convert
    :type event: pixel_logs.Event
    """
    ret = event.to_dict()
    timestamp_fields = ["session.last_session_timestamp", "session.timestamp",
                        "timestamp_info.nginx_ms", "timestamp_info.override_ms",
                        "timestamp_info.pixel_ms"]
    for k, v in iteritems(ret):
        # convert timestamps to seconds
        if v and k in timestamp_fields:
            ret[k] = v / 1000
    return {"json": ret}


def write_events_bigquery(events,
                          bq_conn=None,
                          project_id=None,
                          dataset_id=None,
                          table_id=None):
    """Write a stream of events to BigQuery

    :param bq_conn: The BigQuery connection to write to
    :type bq_conn: googleapiclient.discovery.Resource
    :param project_id: The BigQuery project ID to write to
    :type project_id: str
    :param dataset_id: The BigQuery dataset ID to write to
    :type dataset_id: str
    :param table_id: The BigQuery table ID to write to
    :type table_id: str
    """
    insert_body = {
        "kind": "bigquery#tableDataInsertAllRequest",
        "skipInvalidRows": False,
        "ignoreUnknownValues": False,
        "rows": [event_to_bigquery(event) for event in events]
    }
    if bq_conn is None:
        pprint.PrettyPrinter(indent=0).pprint(insert_body)
        return False
    query = bq_conn.tabledata().insertAll(projectId=project_id,
                                          datasetId=dataset_id,
                                          tableId=table_id,
                                          body=insert_body)
    response = query.execute(num_retries=5)
    if 'insertErrors' in response:
        for error_set in response['insertErrors']:
            for error in error_set['errors']:
                log.error(error)
        return False
    return True


def load_batch_bigquery(network,
                        s3_prefix="",
                        access_key_id="",
                        secret_access_key="",
                        region_name="us-east-1",
                        project_id=None,
                        dataset_id=None,
                        table_id=None,
                        dry_run=False):
    """Load a batch of events from S3 to BigQuery

    :param network: The Parse.ly network for which to perform writes (eg
        "parsely-blog")
    :type network: str
    :param s3_prefix: The S3 timestamp directory prefix from which to fetch data
        batches, formatted as YYYY/MM/DD
    :type s3_prefix: str
    :param access_key_id: The AWS access key to use when fetching data batches
    :type access_key_id: str
    :param secret_access_key: The AWS secret key to use when fetching data batches
    :type secret_access_key: str
    :param region_name: The AWS region in which to perform fetches
    :type region_name: str
    :param project_id: The BigQuery project ID to write to
    :type project_id: str
    :param dataset_id: The BigQuery dataset ID to write to
    :type dataset_id: str
    :param table_id: The BigQuery table ID to write to
    :type table_id: str
    :param dry_run: If True, don't perform BigQuery writes
    :type dry_run: bool
    """
    bq_conn = None
    if not dry_run:
        bq_conn = google_build(
            'bigquery', 'v2',
            credentials=GoogleCredentials.get_application_default())
    s3_stream = events_s3(network, prefix=s3_prefix, access_key_id=access_key_id,
                          secret_access_key=secret_access_key,
                          region_name=region_name)

    def chunked(seq, chunk_size):
        chunk = []
        for item in seq:
            chunk.append(item)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
    for events in chunked(s3_stream, 500):
        write_events_bigquery(events, bq_conn=bq_conn, project_id=project_id,
                              dataset_id=dataset_id, table_id=table_id)


def create_bigquery_table(project_id, table_id, dataset_id):
    """Create a BigQuery table using a schema compatible with Parse.ly events

    :param project_id: The BigQuery project ID to write to
    :type project_id: str
    :param table_id: The BigQuery table ID to write to
    :type table_id: str
    :param dataset_id: The BigQuery dataset ID to write to
    :type dataset_id: str
    """
    schema = {
        "description": "Parse.ly event data",
        "schema": {"fields": [
            {"name": "url", "mode": "REQUIRED", "type": "STRING"},
            {"name": "apikey", "mode": "REQUIRED", "type": "STRING"},
            {"name": "action", "mode": "NULLABLE", "type": "STRING"},
            {"name": "display_avail_height", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "display_avail_width", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "display_pixel_depth", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "display_total_height", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "display_total_width", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "engaged_time_inc", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "extra_data", "mode": "NULLABLE", "type": "STRING"},
            {"name": "referrer", "mode": "NULLABLE", "type": "STRING"},
            {"name": "session_id", "mode": "NULLABLE", "type": "STRING"},
            {"name": "session_initial_referrer", "mode": "NULLABLE", "type": "STRING"},
            {"name": "session_initial_url", "mode": "NULLABLE", "type": "STRING"},
            {"name": "session_last_session_timestamp", "mode": "NULLABLE",
                "type": "TIMESTAMP"},
            {"name": "session_timestamp", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "timestamp_info_nginx_ms", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "timestamp_info_override_ms", "mode": "NULLABLE",
                "type": "TIMESTAMP"},
            {"name": "timestamp_info_pixel_ms", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "user_agent", "mode": "NULLABLE", "type": "STRING"},
            {"name": "visitor_ip", "mode": "NULLABLE", "type": "STRING"},
            {"name": "visitor_network_id", "mode": "NULLABLE", "type": "STRING"},
            {"name": "visitor_site_id", "mode": "NULLABLE", "type": "STRING"}
        ]},
        "tableReference": {
            "projectId": project_id,
            "tableId": table_id,
            "datasetId": dataset_id
        }
    }
    credentials = GoogleCredentials.get_application_default()
    bigquery = google_build('bigquery', 'v2', credentials=credentials)
    bigquery.tables().insert(projectId=project_id,
                             datasetId=dataset_id,
                             tableId=table_id,
                             body=schema).execute()


def main():
    commands = ["load_batch_bigquery", "create_bigquery_table"]
    parser = utils.get_default_parser("Google BigQuery utilities for Parse.ly",
                                      commands=commands)
    parser.add_argument('--dry_run', action="store_true",
                        help="If true, don't perform writes to Bigquery"
                             'connect, ending in "redshift.amazonaws.com"')
    parser.add_argument('bigquery_project_id', type=str,
                        help='The ID of the BigQuery project to which to connect')
    parser.add_argument('bigquery_dataset_id', type=str,
                        help='The ID of the BigQuery dataset to which to connect')
    parser.add_argument('bigquery_table_id', type=str,
                        help='The ID of the BigQuery table to which to connect')
    args = parser.parse_args()

    if args.command == "load_batch_bigquery":
        load_batch_bigquery(
            args.network,
            s3_prefix=args.s3_prefix,
            access_key_id=args.aws_access_key_id,
            secret_access_key=args.aws_secret_access_key,
            region_name=args.aws_region_name,
            project_id=args.bigquery_project_id,
            dataset_id=args.bigquery_dataset_id,
            table_id=args.bigquery_table_id,
            dry_run=args.dry_run
        )
    elif args.command == "create_bigquery_table":
        create_bigquery_table(
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            table_id=args.table_id
        )

if __name__ == "__main__":
    main()
