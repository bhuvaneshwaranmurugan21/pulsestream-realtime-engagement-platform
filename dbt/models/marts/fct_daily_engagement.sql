{{ config(unique_key=['event_date', 'event_type']) }}

select
    cast(event_time as date) as event_date,
    event_type,
    count(*) as event_count,
    count(distinct user_token) as unique_users,
    max(arrival_time) as data_fresh_through
from {{ ref('stg_engagement_event') }}
{% if is_incremental() %}
where event_time >= dateadd(day, -3, (select coalesce(max(event_date), '1900-01-01') from {{ this }}))
{% endif %}
group by 1, 2
