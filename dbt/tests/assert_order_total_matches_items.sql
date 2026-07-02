-- 정합성 체크: orders.total_amount가 실제 order_items 합계와 일치하는지 검증.
-- 결과 행이 하나라도 있으면 실패(dbt test 컨벤션).
select
    o.order_id,
    o.total_amount as order_total,
    sum(oi.quantity * oi.unit_price) as items_total
from {{ ref('stg_orders') }} o
join {{ ref('stg_order_items') }} oi on oi.order_id = o.order_id
group by o.order_id, o.total_amount
having abs(o.total_amount - sum(oi.quantity * oi.unit_price)) > 0.01
