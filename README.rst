.. image:: https://travis-ci.org/Parsely/parsely_raw_data.svg?branch=master
    :target: https://travis-ci.org/Parsely/parsely_raw_data

Parse.ly Raw Data
=================

This repository contains Python example code for working with Parse.ly's
"raw data API". This "API" comprises a collection of AWS tools that Parse.ly
customers can use to gain batch and streaming access to the raw data that Parse.ly
collects from their sites. Streaming access is provided via Amazon Kinesis Streams,
and batch access via Amazon S3.

To set up Raw Data API access for your Parse.ly account, please contact
Parse.ly support.

You can download this repository with

::

    $ pip install parsely_raw_data

The files in this module are named for the services they interface with:

* `s3.py`: Fetch event data from a Parse.ly-managed S3 bucket
* `stream.py`: Consume a Kinesis Stream of realtime Parse.ly event data
* `redshift.py`: Copy data from S3 to an Amazon Redshift instance
* `bigquery.py`: Write event data to a Google BigQuery table

This repository also contains a Thrift-based abstraction over a Parse.ly event
called `Event`. This class is used to standardize the format of events
passed around between various services.
