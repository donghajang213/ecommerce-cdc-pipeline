-- CDC 이벤트 중 주문별 최신 상태(상태값 CREATED->PAID->SHIPPED->DELIVERED 반영)만 남긴 뷰
select
    order_id,
    user_id,
    status,
    total_amount,
    created_at,
    updated_at
from {{ source('raw', 'orders_raw') }}
where not __deleted
qualify row_number() over (partition by order_id order by __ts_ms desc, __seq desc) = 1
