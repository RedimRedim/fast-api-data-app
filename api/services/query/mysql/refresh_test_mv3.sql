drop table if exists mv.test_mv3;

CREATE table mv.test_mv3 as
with birth_date_year as (
select extract(year from birth_date) as year , count(birth_date) as cnt from `mysql`.`users`
group by extract(year from birth_date) with rollup
order by extract(year from birth_date) asc)


select 
case when year is null then 'total' else year end as year,
cnt
from birth_date_year;
