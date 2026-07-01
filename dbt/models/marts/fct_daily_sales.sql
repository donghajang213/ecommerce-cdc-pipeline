-- 일별 매출 마트: BI 대시보드의 매출 추이 차트에 바로 쓸 수 있는 요약 테이블
select
    date_trunc('day', created_at) as order_date,
    count(distinct order_id) as order_count,
    sum(total_amount) as revenue,
    round(sum(total_amount) / nullif(count(distinct order_id), 0), 2) as avg_order_value
from {{ ref('stg_orders') }}
group by 1
order by 1
