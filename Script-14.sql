--запрос для 1 товара для корреляции между динамикой выручки и динамикой показов
select qq.id, qq.sku_id, qq.name, qq.date, qq.revenue, qq.pct_change_revenue,
qq.hits_view, qq.pct_change_view, qq.revenue_to_view,
avg(qq.revenue_to_view) over () as avg_corr_to_view
from (select q.id, q.sku_id, q.name, q.date,
q.hits_view, coalesce((q.hits_view - q.prev_value_view) * 100 /nullif(q.prev_value_view, 0), 0) as pct_change_view,
q.revenue, coalesce((q.revenue - q.prev_value_revenue) * 100 /nullif(q.prev_value_revenue, 0), 0) as pct_change_revenue,
coalesce(coalesce((q.revenue - q.prev_value_revenue) * 100 /nullif(q.prev_value_revenue, 0), 0) 
/nullif(coalesce((q.hits_view - q.prev_value_view) * 100 /nullif(q.prev_value_view, 0), 0), 0), 0) as revenue_to_view
from (select tda.id, tda.sku_id, tda.name, tda.date, tda.hits_view,
lag(tda.hits_view) over (partition by tda.sku_id order by tda.date
rows between 1 preceding and current row) as prev_value_view,
tda.revenue, lag(tda.revenue) over (partition by tda.sku_id order by tda.date
rows between 1 preceding and current row) as prev_value_revenue
from total_data_analitics tda
where tda.sku_id = 285234969) as q) as qq
where qq.pct_change_revenue != 0
order by avg(qq.revenue_to_view) over (partition by qq.sku_id) desc

----запрос всех товаров отдельного продавца для корреляции между динамикой выручки и динамикой показов
select qqqq.sku_id, qqqq.name,
avg(qqqq.revenue_to_view) over (partition by qqqq.sku_id) as avg_corr_to_view,
qqqq.date,
qqqq.revenue, qqqq.pct_change_revenue,
qqqq.hits_view, qqqq.pct_change_view, qqqq.revenue_to_view, qqqq.id
from (select qqq.id, qqq.sku_id, qqq.name, qqq.date,
qqq.hits_view, coalesce((qqq.hits_view - qqq.prev_value_view) * 100 /nullif(qqq.prev_value_view, 0), 0) as pct_change_view,
qqq.revenue, coalesce((qqq.revenue - qqq.prev_value_revenue) * 100 /nullif(qqq.prev_value_revenue, 0), 0) as pct_change_revenue,
coalesce(coalesce((qqq.revenue - qqq.prev_value_revenue) * 100 /nullif(qqq.prev_value_revenue, 0), 0)
/nullif(coalesce((qqq.hits_view - qqq.prev_value_view) * 100 /nullif(qqq.prev_value_view, 0), 0), 0), 0) as revenue_to_view
from (select qq.id, qq.sku_id, qq.name, qq.date, qq.hits_view,
lag(qq.hits_view) over (partition by qq.sku_id order by qq.date
rows between 1 preceding and current row) as prev_value_view,
qq.revenue, lag(qq.revenue) over (partition by qq.sku_id order by qq.date
rows between 1 preceding and current row) as prev_value_revenue
from (select q.id, q.sku_id, q.name, q.date, q.hits_view, q.revenue
from (select tda.id, tda.sku_id, tda.name, tda.date, tda.hits_view, tda.revenue,
max(tda.id) over (partition by tda.sku_id, tda.date) as max_id
from total_data_analitics tda
where tda.api_id = '133183') as q
where q.id = q.max_id) as qq) as qqq)as qqqq
order by avg(qqqq.revenue_to_view) over (partition by qqqq.sku_id) desc

--запрос для создания выбросов в ряду средних корреляции между динамикой выручки и динамикой показов
with baza as (select qqqq.sku_id, qqqq.name,
avg(qqqq.revenue_to_view) over (partition by qqqq.sku_id) as avg_corr_to_view,
qqqq.date,
qqqq.revenue, qqqq.pct_change_revenue,
qqqq.hits_view, qqqq.pct_change_view, qqqq.revenue_to_view, qqqq.id
from (select qqq.id, qqq.sku_id, qqq.name, qqq.date,
qqq.hits_view, coalesce((qqq.hits_view - qqq.prev_value_view) * 100 /nullif(qqq.prev_value_view, 0), 0) as pct_change_view,
qqq.revenue, coalesce((qqq.revenue - qqq.prev_value_revenue) * 100 /nullif(qqq.prev_value_revenue, 0), 0) as pct_change_revenue,
coalesce(coalesce((qqq.revenue - qqq.prev_value_revenue) * 100 /nullif(qqq.prev_value_revenue, 0), 0)
/nullif(coalesce((qqq.hits_view - qqq.prev_value_view) * 100 /nullif(qqq.prev_value_view, 0), 0), 0), 0) as revenue_to_view
from (select qq.id, qq.sku_id, qq.name, qq.date, qq.hits_view,
lag(qq.hits_view) over (partition by qq.sku_id order by qq.date
rows between 1 preceding and current row) as prev_value_view,
qq.revenue, lag(qq.revenue) over (partition by qq.sku_id order by qq.date
rows between 1 preceding and current row) as prev_value_revenue
from (select q.id, q.sku_id, q.name, q.date, q.hits_view, q.revenue
from (select tda.id, tda.sku_id, tda.name, tda.date, tda.hits_view, tda.revenue,
max(tda.id) over (partition by tda.sku_id, tda.date) as max_id
from total_data_analitics tda
where tda.api_id = '133183' and tda.date != '2022-52') as q
where q.id = q.max_id) as qq) as qqq)as qqqq
order by avg(qqqq.revenue_to_view) over (partition by qqqq.sku_id) desc),
iqr as (select percentile_disc (0.75) within group (order by series.avg_corr_to_view) as third_qu,
percentile_disc (0.25) within group (order by series.avg_corr_to_view) as first_qu
from (select distinct baza.avg_corr_to_view
from baza) as series)
select baza.*,
case when (baza.avg_corr_to_view > (select iqr.first_qu from iqr))
and (baza.avg_corr_to_view < (select iqr.third_qu from iqr))
then 'False'
else 'True'
end outlier
from baza

