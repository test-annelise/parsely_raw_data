select * from {{ var('parsely:events') }}
where action in ('videostart','vheartbeat')

-- Questions
-- where is it best to do where: action = pageview, heartbeat, videostart, vheartbeat?
-- how/when to handle metadata updates to posts? since that lives for each row on the pageviews and videoviews tables?
---- maybe have a content dim?