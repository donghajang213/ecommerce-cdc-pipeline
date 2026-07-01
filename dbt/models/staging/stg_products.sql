-- CDC 이벤트 중 상품별 최신 상태(가격 변경 반영)만 남긴 뷰
select
    product_id,
    sku,
    name,
    category,
    price,
    created_at,
    updated_at
from {{ source('raw', 'products_raw') }}
where not __deleted
qualify row_number() over (partition by product_id order by __ts_ms desc) = 1
