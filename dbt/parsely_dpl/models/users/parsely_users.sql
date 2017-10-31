-- 1 row per visitor_site_Id
-- includes visitor type, returning, new, subscribers, etc
-- first login, last login, etc

{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='apikey_visitor_id'
    )
}}

-- second time: builds temp table; deletes duplicates by unique key, inserts new data
--


with incoming_users as (
  select
      apikey,
      apikey_visitor_id,
      visitor_site_id,
      visitor_ip,
      {{ var('custom:extradataname') }},
      -- dates and times
      max(last_timestamp) as last_timestamp,
      date(min(last_timestamp)) as date_first_seen,
      date(max(last_timestamp)) as date_last_seen,
      -- metrics to aggregate
      sum(user_total_pageviews) as user_total_pageviews,
      sum(user_total_engaged_time) as user_total_engaged_time,
      sum(user_total_videoviews) as user_total_videoviews,
      sum(user_total_video_engaged_time) as user_total_video_engaged_time
  from {{ref('parsely_incoming_users')}}
  group by 1,2,3,4,5
),

{%if adapter.already_exists(this.schema,this.name)%}

relevant_existing as (

    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        visitor_ip,
        -- current user type
        iu.{{ var('custom:extradataname') }},
        -- dates and times
        eu.last_timestamp,
        eu.date_first_seen,
        eu.date_last_seen,
        -- metrics to aggregate
        eu.user_total_pageviews,
        eu.user_total_engaged_time,
        eu.user_total_videoviews,
        eu.user_total_video_engaged_time
    from {{ this }} as eu
    left join incoming_users as iu using (apikey_visitor_id, apikey, visitor_site_id, visitor_ip)
    --where apikey_visitor_id in (select apikey_visitor_id from incoming_users)

),

-- left join fields from old data: min_tstamp
unioned as (

    -- combined pageviews and videostarts
    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        visitor_ip,
        {{ var('custom:extradataname') }},
        -- dates and times
        last_timestamp,
        date_first_seen,
        date_last_seen,
        -- metrics to aggregate
        user_total_pageviews,
        user_total_engaged_time,
        user_total_videoviews,
        user_total_video_engaged_time
    from incoming_users

    union all

    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        visitor_ip,
        --incoming users fields
        {{ var('custom:extradataname') }},
        -- dates and times
        last_timestamp,
        date_first_seen,
        date_last_seen,
        -- metrics to aggregate
        user_total_pageviews,
        user_total_engaged_time,
        user_total_videoviews,
        user_total_video_engaged_time
    from relevant_existing

),

merged as (

    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        visitor_ip,
        --incoming users fields
        {{ var('custom:extradataname') }},
        -- dates and times
        max(last_timestamp) as last_timestamp,
        min(date_first_seen) as date_first_seen,
        max(date_last_seen) as date_last_seen,
        -- metrics
        sum(user_total_pageviews) as user_total_pageviews,
        sum(user_total_engaged_time) as user_total_engaged_time,
        sum(user_total_videoviews) as user_total_videoviews,
        sum(user_total_video_engaged_time) as user_total_video_engaged_time
    from unioned
    group by 1,2,3,4,5


)

{% else %}

-- initial run, don't merge
merged as (

    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        visitor_ip,
        {{ var('custom:extradataname') }},
        -- dates and times
        max(last_timestamp) as last_timestamp,
        date(min(last_timestamp)) as date_first_seen,
        date(max(last_timestamp)) as date_last_seen,
        -- metrics to aggregate
        sum(user_total_pageviews) as user_total_pageviews,
        sum(user_total_engaged_time) as user_total_engaged_time,
        sum(user_total_videoviews) as user_total_videoviews,
        sum(user_total_video_engaged_time) as user_total_video_engaged_time
    from incoming_users
    group by 1,2,3,4,5
)

{% endif %}

select
    apikey,
    apikey_visitor_id,
    visitor_site_id,
    visitor_ip,
    {{ var('custom:extradataname') }},
    -- dates and times
    last_timestamp,
    date_first_seen,
    date_last_seen,
    -- metrics to aggregate
    user_total_pageviews,
    user_total_engaged_time,
    user_total_videoviews,
    user_total_video_engaged_time,
    -- derived fields
    case when date_first_seen < date(SYSDATE) then 'Returning' else 'New' end as user_type,
    case when user_total_pageviews>={{var('custom:loyaltyuser')}} then 'Loyalty' else 'Non-Loyalty' end as user_engagement_level,
    DATEDIFF(day, last_timestamp, SYSDATE) as days_since_last_session
  from merged
