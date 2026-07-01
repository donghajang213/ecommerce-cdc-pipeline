-- 주문 상세 항목 (생성 이후 변경되지 않는 테이블이지만 안전하게 최신본만 사용)
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at
from {{ source('raw', 'order_items_raw') }}
where not __deleted
qualify row_number() over (partition by order_item_id order by __ts_ms desc) = 1
