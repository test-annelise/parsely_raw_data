"""IDL file as Python module."""
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

IDL = """
# Thrift IDL files are described in good detail here:
# http://diwakergupta.github.io/thrift-missing-guide/


struct VisitorInfo {
    1: required string network_id;
    2: required string site_id;
    3: required string ip;
}


struct DisplayInfo {
    1: required i16 total_width;
    2: required i16 total_height;
    3: optional i16 avail_width;
    4: optional i16 avail_height;
    5: required i16 pixel_depth;
}


struct TimestampInfo {
    1: required i64 nginx_ms;
    2: optional i64 pixel_ms;
    3: optional i64 override_ms;
}


struct SessionInfo {
    1: required i16 id;
    2: required i64 timestamp;
    3: required string initial_url;
    4: optional string initial_referrer;
    5: required i64 last_session_timestamp;
}


struct LogLine {
    1: required string apikey;
    2: required string url;
    3: optional string referrer;
    4: optional string action;
    5: optional i16 engaged_time_inc;
    6: required VisitorInfo visitor;
    7: optional map<string, string> extra_data;
    8: required string user_agent;
    9: optional DisplayInfo display;
    10: required TimestampInfo timestamp_info;
    11: optional SessionInfo session;
}
"""
