-- CDC 이벤트 중 상품별 최신 재고 수량만 남긴 뷰
select
    product_id,
    quantity,
    updated_at
from {{ source('raw', 'inventory_raw') }}
where not __deleted
qualify row_number() over (partition by product_id order by __ts_ms desc) = 1
