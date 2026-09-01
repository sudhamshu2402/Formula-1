-- Databricks notebook source
create or replace view formula1.gold.constructor_stats as

with constructors_stat as (
select 
    r.season,
    c.constructor_id,
    c.constructor_name,
    c.nationality,
    count(*) as races,
    sum(r.points) as total_points,
    count_if(r.is_win) as number_of_wins,
    count_if(r.is_podium) as number_of_podiums
from 
    formula1.gold.facts_results r
join 
    formula1.gold.dim_constructors c
on 
    r.constructor_id = c.constructor_id
group by 
    r.season,c.constructor_id,c.constructor_name, c.nationality
)

select
    rank() over(partition by season order by total_points desc, number_of_wins desc) as ranks,
    season,
    constructor_id,
    constructor_name,
    nationality,
    races,
    total_points,
    number_of_wins,
    number_of_podiums
from 
    constructors_stat


-- COMMAND ----------

select * from formula1.gold.constructor_stats
where season = 2025