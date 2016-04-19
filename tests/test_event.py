from parsely_raw_data.event import (Event, VisitorInfo, TimestampInfo,
                                    DisplayInfo, SessionInfo)


event = Event(
    apikey='example.com',
    url='http://localhost:8000/examples.build/analytics.amp.html',
    visitor=VisitorInfo(
        network_id=None,
        ip='198.200.78.40',
        site_id='amp-kiZ6-WZla1kXFFWAw2oxfLyJVWB8ytaeJ7ghXsXe5'),
    engaged_time_inc=None,
    timestamp_info=TimestampInfo(
        override_ms=None,
        pixel_ms=1455027478791,
        nginx_ms=1451685961000),
    session=SessionInfo(
        id=12345,
        timestamp=1457737463000,
        initial_url="http://test.com",
        initial_referrer="http://test2.com",
        last_session_timestamp=1457737463000
    ),
    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36'
               ' (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36',
    referrer=None,
    action='pageview',
    extra_data=None,
    display=DisplayInfo(
        total_width=1920,
        avail_height=1177,
        avail_width=1916,
        total_height=1200,
        pixel_depth=24)
)

def test_binary():
    assert Event.from_binary(event.to_binary()) == event

def test_dict():
    assert Event.from_dict(event.to_dict()) == event
