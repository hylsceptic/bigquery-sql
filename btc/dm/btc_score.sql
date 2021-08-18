truncate table `chaingraph-318604.btc_dm.btc_address_score`;

insert into `chaingraph-318604.btc_dm.btc_address_score`

-- create or replace table `chaingraph-318604.btc_dm.btc_address_score` as 
-- PARTITION BY
--     RANGE_BUCKET(pt, GENERATE_ARRAY(0, 3999, 1))
-- cluster by
--     address
-- as

with 
all_tag_addresses as
(
    select 
        *
    from
    (
        SELECT distinct 
            address,
            plots,
            idx,
            max(sub_weight) as sub_weight,
        FROM `chaingraph-318604.btc_tags.generated_tags`
        group by address, plots, idx
    ) 
    union all 
    (
        select distinct 
            address,
            plots,
            idx,
            1 as  sub_weight
        from `chaingraph-318604.btc_tags.btc_union_tags`
    )
),

plots_list as 
(
    select 
        address,
        array_agg(concat(plots, "_", cast(idx AS string))) as plots_list
    from 
    all_tag_addresses group by address
),


score_table as 
(
    select 
        feature_no as plots,
        feature_weight as weight,
        subfeature_no as idx,
        subfeature_score as idx_score
    from `chaingraph-318604.btc_data.risk_plots`
),

sum_address_score as
(
    SELECT
        address,
        plots,
        sum(sub_weight * weight * idx_score) as sub_weight
    FROM all_tag_addresses left join score_table using(plots, idx) 
    where weight is not null
    group by address, plots
)

select
    *,
    CURRENT_TIMESTAMP() as generated_timestamp,
    mod(cast(concat('0x', substr(cast(md5(address) as string format 'hex'), -3)) as int64), 4000) as pt
from 
(
    select
        address,
        sum(sub_weight) as total_score, 
        1 / (1 + pow(2.718281828459045, -(sum(sub_weight) - 60) / 30)) as risk_score,
        array_agg(concat(plots, '_', cast((sub_weight) as string))) as plot_weight
    from sum_address_score
    group by address
) left join plots_list using(address)
-- -- order by total_score desc
