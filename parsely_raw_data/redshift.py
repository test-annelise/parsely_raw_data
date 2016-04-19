from __future__ import absolute_import, print_function

import psycopg2

from . import utils

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


def create_table(host="", user="", password="", database="", port="5439"):
    """Create a Redshift table using a schema compatible with Parse.ly events

    :param host: The Redshift host on which to create the table
    :type host: str
    :param user: The Redshift user to use when creating the table
    :type user: str
    :param password: The Redshift password to use when creating the table
    :type password: str
    :param database: The name of the Redshift database on which to create the table
    :type database: str
    :param port: The port on which to connect to Redshift
    :type port: str
    """
    query = """
CREATE TABLE events
(
    url                             VARCHAR(4096) NOT NULL,
    apikey                          VARCHAR(256) NOT NULL,
    action                          VARCHAR(256),
    display_avail_height            INTEGER,
    display_avail_width             INTEGER,
    display_pixel_depth             INTEGER,
    display_total_height            INTEGER,
    display_total_width             INTEGER,
    engaged_time_inc                INTEGER,
    extra_data                      VARCHAR(4096),
    referrer                        VARCHAR(1024),
    session_id                      VARCHAR(256)
    session_initial_referrer        VARCHAR(512),
    session_initial_url             VARCHAR(512),
    session_last_session_timestamp  TIMESTAMP,
    session_timestamp               TIMESTAMP,
    timestamp_info_nginx_ms         TIMESTAMP,
    timestamp_info_override_ms      TIMESTAMP,
    timestamp_info_pixel_ms         TIMESTAMP,
    user_agent                      VARCHAR(512),
    visitor_ip                      VARCHAR(128),
    visitor_network_id              VARCHAR(128),
    visitor_site_id                 VARCHAR(128)
);
"""
    connection = psycopg2.connect(host=host, port=port, user=user,
                                  password=password, database=database)
    connection.cursor().execute(query)
    connection.commit()


def copy_from_s3(network,
                 s3_prefix,
                 host="",
                 user="",
                 password="",
                 database="",
                 port="5439",
                 access_key_id="",
                 secret_access_key=""):
    """Use the Redshift COPY command to copy event data from S3

    :param network: The Parse.ly network for which to copy data (eg
        "parsely-blog")
    :type network: str
    :param s3_prefix: The S3 timestamp directory prefix from which to fetch data
        batches, formatted as YYYY/MM/DD
    :type s3_prefix: str
    :param host: The Redshift host to which to write
    :type host: str
    :param user: The Redshift user to use when writing
    :type user: str
    :param password: The Redshift password to use when writing
    :type password: str
    :param database: The name of the Redshift database to which to write
    :type database: str
    :param port: The port on which to connect to Redshift
    :type port: str
    :param access_key_id: The AWS access key to use when copying
    :type access_key_id: str
    :param secret_access_key: The AWS secret key to use when copying
    :type secret_access_key: str
    """
    connection = psycopg2.connect(host=host, port=port, user=user,
                                  password=password, database=database)
    query = "copy visits\n"
    "from 's3://parsely-dw-{network}/{s3_prefix}'\n"
    "credentials 'aws_access_key_id={aws_access_key_id};"
    "aws_secret_access_key={aws_secret_access_key}'\n"
    "json as 'auto' gzip;".format(
        network=utils.clean_network(network),
        s3_prefix=s3_prefix,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key)
    connection.cursor().execute(query)
    connection.commit()


def main():
    commands = ['copy_from_s3', 'create_table']
    parser = utils.get_default_parser("Amazon Redshift utilities for Parse.ly",
                                      commands=commands)
    parser.add_argument('--redshift_host', type=str,
                        help='The host string of the Redshift instance to which to '
                             'connect, ending in "redshift.amazonaws.com"')
    parser.add_argument('--redshift_user', type=str,
                        help='The user name to use when connecting to the Redshift'
                             ' instance')
    parser.add_argument('--redshift_password', type=str,
                        help='The password to use when connecting to the Redshift'
                             ' instance')
    parser.add_argument('--redshift_database', type=str,
                        help='The Redshift database to which to connect')
    parser.add_argument('--redshift_port', type=str,
                        help='The port on which to connect to Redshift')
    args = parser.parse_args()

    if args.command == "copy_from_s3":
        copy_from_s3(
            args.network,
            args.s3_prefix,
            host=args.redshift_host,
            user=args.redshift_user,
            password=args.redshift_password,
            database=args.redshift_database,
            access_key_id=args.aws_access_key_id,
            secret_access_key=args.aws_secret_access_key
        )
    elif args.command == "create_table":
        create_table(
            host=args.redshift_host,
            user=args.redshift_user,
            password=args.redshift_password,
            database=args.redshift_database,
            port=args.redshift_port
        )

if __name__ == "__main__":
    main()
