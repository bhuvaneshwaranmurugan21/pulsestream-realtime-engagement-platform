select
    event_id,
    source_id,
    user_token,
    event_type,
    cast(event_time as timestamp) as event_time,
    cast(arrival_time as timestamp) as arrival_time,
    consent_state,
    page_id,
    campaign_id
from {{ source('lakehouse', 'curated_event') }}
where consent_state = 'analytics_allowed'
