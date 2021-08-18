declare excute_day date;
set excute_day = date("2020-01-01");

truncate table `chaingraph-318604.ethereum_tags.generated_tags`;
insert into `chaingraph-318604.ethereum_tags.generated_tags`

-- create or replace table `chaingraph-318604.ethereum_tags.generated_tags` as
with 
succeed_transfers as
(
    select * from `chaingraph-318604.ethereum_data.eth_succeed_transfers` 
    -- where pt = excute_day
    where pt <= date_sub(date(current_timestamp()), interval 1 day)
),

taged_addresses as (
    select distinct address, plots from `chaingraph-318604.ethereum_tags.eth_address_tags`
),

total_spent as (
    select 
        from_address as address,
        sum(value) as spent_value,
        sum(usd_value) as spent_usd
    from succeed_transfers group by from_address
),

total_receive as (
    select 
        to_address as address,
        sum(value) as receive_value,
        sum(usd_value) as receive_usd
    from succeed_transfers group by to_address
),

direct_source as (
    select
        -- a.hash,
        a.from_address as address,
        b.plots,
        3 as idx,
        sum(a.usd_value) / max(c.spent_usd + 1e6) as sub_weight,
    from succeed_transfers as a,
        taged_addresses as b,
        total_spent as c
        where a.to_address = b.address and a.from_address = c.address
    group by a.from_address, b.plots
),

direct_dest as (
    select
        -- a.hash,
        a.to_address as address,
        b.plots,
        4 as idx,
        sum(a.usd_value) / max(c.receive_usd + 1e6) as sub_weight,
    from succeed_transfers as a,
        taged_addresses as b,
        total_receive as c
        where a.from_address = b.address and a.to_address = c.address
    group by a.to_address, b.plots
)

-- select * from total_receive where receive_usd = 0
select * from
(
    select 
        address,
        plots,
        idx,
        max(sub_weight) as sub_weight
    from direct_dest group by address, plots, idx
    union all 
    select 
        address,
        plots,
        idx,
        max(sub_weight) as sub_weight
    from direct_source group by address, plots, idx
) 


