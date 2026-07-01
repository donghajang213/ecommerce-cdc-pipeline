-- 상품별 판매 마트: 어떤 상품/카테고리가 잘 팔리는지 보는 마트
select
    p.product_id,
    p.sku,
    p.name as product_name,
    p.category,
    count(distinct oi.order_id) as order_count,
    sum(oi.quantity) as units_sold,
    sum(oi.quantity * oi.unit_price) as revenue,
    i.quantity as current_stock
from {{ ref('stg_order_items') }} as oi
inner join {{ ref('stg_products') }} as p on oi.product_id = p.product_id
left join {{ ref('stg_inventory') }} as i on p.product_id = i.product_id
group by 1, 2, 3, 4, i.quantity
order by revenue desc
