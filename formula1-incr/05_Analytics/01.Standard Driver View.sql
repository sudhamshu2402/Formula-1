-- Databricks notebook source
create or replace view formula1.gold.v_driver as

with driver_season_summary as (

select r.season,
        r.driver_id,
        d.driver_name,
        d.nationality,
        count(*) as races_starts,
        sum(r.points) as total_points,
        count_if(r.is_win) as number_of_wins,
        count_if(r.is_podium) as number_of_podiums
    from formula1.gold.facts_results r
join formula1.gold.dim_drivers d on r.driver_id = d.driver_id
group by r.season,
        r.driver_id,
        d.driver_name,
        d.nationality
order by r.season,
        total_points desc
)

select 
        rank() over(partition by season order by total_points desc,number_of_wins desc) as standing,
        season,
        driver_id,
        driver_name,
        nationality,
        races_starts,
        total_points,
        number_of_wins,
        number_of_podiums
    from driver_season_summary

-- COMMAND ----------

select * from formula1.gold.v_driver
where season = 2025