from __future__ import absolute_import, print_function, division

import io
import struct

import thriftpy
import thriftpy.protocol.binary as binaryproto
from pkg_resources import resource_stream
from six import PY3

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
# Load thrift IDL module
with resource_stream(__name__, "event.thrift") as thrift_file:
    if PY3:
        thrift_file = io.TextIOWrapper(thrift_file, encoding='utf-8')
    event_thrift = thriftpy.load_fp(thrift_file, 'event_thrift')


LINE_DELIMITER = '\x02\n\x03'
# Surface some of the classes from the IDL
VisitorInfo = event_thrift.VisitorInfo
TimestampInfo = event_thrift.TimestampInfo
SessionInfo = event_thrift.SessionInfo
DisplayInfo = event_thrift.DisplayInfo


class Event(event_thrift.Event):
    def to_binary(self):
        buf = io.BytesIO()
        binaryproto.TBinaryProtocol(buf).write_struct(self)
        return buf.getvalue()

    @classmethod
    def from_binary(cls, binary):
        buf = io.BytesIO(binary)
        output = cls()
        binaryproto.read_struct(buf, output)
        return output

    def to_dict(self):
        """Return a Event represented as a dictionary."""
        return {
            'apikey': self.apikey,
            'url': self.url,
            'referrer': self.referrer,
            'action': self.action,
            'engaged_time_inc': self.engaged_time_inc,
            'extra_data': self.extra_data,
            'visitor_site_id': self.visitor.site_id if self.visitor else '',
            'visitor_network_id': self.visitor.network_id if self.visitor else '',
            'visitor_ip': self.visitor.ip if self.visitor else '',
            'user_agent': self.user_agent,
            'display_total_width': self.display.total_width if self.display else None,
            'display_total_height': self.display.total_height if self.display else None,
            'display_avail_width': self.display.avail_width if self.display else None,
            'display_avail_height': self.display.avail_height if self.display else None,
            'display_pixel_depth': self.display.pixel_depth if self.display else None,
            'timestamp_info_nginx_ms': (self.timestamp_info.nginx_ms
                                        if self.timestamp_info else None),
            'timestamp_info_pixel_ms': (self.timestamp_info.pixel_ms
                                        if self.timestamp_info else None),
            'timestamp_info_override_ms': (self.timestamp_info.override_ms
                                           if self.timestamp_info else None),
            'session_id': self.session.id if self.session else '',
            'session_timestamp': self.session.timestamp if self.session else None,
            'session_initial_url': self.session.initial_url if self.session else '',
            'session_initial_referrer': (self.session.initial_referrer
                                         if self.session else ''),
            'session_last_session_timestamp': (self.session.last_session_timestamp
                                               if self.session else None)
        }

    @classmethod
    def from_dict(cls, data):
        """Accepts a dictionary formatted as the output of to_dict, returns a new Event
        """
        display = DisplayInfo(total_width=data.get('display_total_width'),
                              total_height=data.get('display_total_height'),
                              avail_width=data.get('display_avail_width'),
                              avail_height=data.get('display_avail_height'),
                              pixel_depth=data.get('display_pixel_depth'))
        visitor = VisitorInfo(site_id=data.get('visitor_site_id'),
                              network_id=data.get('visitor_network_id'),
                              ip=data.get('visitor_ip'))
        timestamp_info = TimestampInfo(nginx_ms=data.get('timestamp_info_nginx_ms'),
                                       pixel_ms=data.get('timestamp_info_pixel_ms'),
                                       override_ms=data.get('timestamp_info_override_ms'))
        session = SessionInfo(
            id=data.get('session_id'),
            timestamp=data.get('session_timestamp'),
            initial_url=data.get('session_initial_url'),
            initial_referrer=data.get('session_initial_referrer'),
            last_session_timestamp=data.get('session_last_session_timestamp'))
        return cls(apikey=data.get('apikey'),
                   url=data.get('url'),
                   referrer=data.get('referrer'),
                   action=data.get('action'),
                   engaged_time_inc=data.get('engaged_time_inc'),
                   extra_data=data.get('data'),
                   visitor=visitor,
                   user_agent=data.get('user_agent'),
                   display=display,
                   timestamp_info=timestamp_info,
                   session=session)

    @classmethod
    def from_binary_file(cls, buff, delimiter=LINE_DELIMITER):
        """Generate Event instances from a file like object."""
        try:
            while True:
                output = cls()
                binaryproto.read_struct(buff, output)
                yield output
                buff.read(len(delimiter))
        except struct.error:
            raise StopIteration
