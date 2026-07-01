-- 사용자별 구매 요약 마트: 재구매율/우수고객 분석의 기초가 되는 마트
select
    u.user_id,
    u.full_name,
    u.email,
    count(distinct o.order_id) as total_orders,
    coalesce(sum(o.total_amount), 0) as lifetime_value,
    min(o.created_at) as first_order_at,
    max(o.created_at) as last_order_at,
    case when count(distinct o.order_id) > 1 then true else false end as is_repeat_customer
from {{ ref('stg_users') }} as u
left join {{ ref('stg_orders') }} as o on u.user_id = o.user_id
group by 1, 2, 3
