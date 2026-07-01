-- CDC 이벤트 중 사용자별 최신 상태만 남긴 뷰
select
    user_id,
    email,
    full_name,
    signup_at,
    updated_at
from {{ source('raw', 'users_raw') }}
where not __deleted
qualify row_number() over (partition by user_id order by __ts_ms desc) = 1
