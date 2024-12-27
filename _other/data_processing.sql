



with cte0 as (

SELECT x::timestamp    as ts0
FROM   generate_series(timestamp '2019-01-01'
           , timestamp '2024-10-05'
           , interval  '1 hour') t(x)
--FROM   generate_series(timestamp '{dt0}'
--                     , timestamp '{dt1}'

), cte1 as (

select ts0
    ,solar
    ,wind
from core.fuel_mix_1_hr_hist

), cte2 as (

select ts0
    ,caiso_load
from core.load_1_hr_hist

), cte7 as (

select cte0.ts0
    --,cte0.ts0::time                               as hr0
    ,cte0.ts0::date::varchar                      as dt0
    ,date_part('hour',cte0.ts0)                   as hr0
    ,cte1.solar
    ,cte1.wind
    ,cte2.caiso_load
    ,cte1.solar + cte1.wind                       as sol_wind
    ,cte2.caiso_load - ( cte1.solar + cte1.wind ) as duck

from cte0
   left join cte1 on cte1.ts0 = cte0.ts0
   left join cte2 on cte2.ts0 = cte0.ts0

)

select *
from cte7
;







