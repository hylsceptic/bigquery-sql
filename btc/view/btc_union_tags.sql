with
btc_address_fast_tx as (
    select 
        address,
        feature,
        name,
        'P112' as plots,
        case idx 
            when 1 then 11 
            when 2 then 12 
            when 3 then 13 
        end as idx
    from 
        `chaingraph-318604.btc_tags.btc_address_fast_tx`
)
,
btc_address_high_fee as (
    select 
        address,
        feature,
        null as name,
        'P121' as plots,
        case idx
            when 1 then 10
            when 2 then 11
        end as idx
    from 
        `chaingraph-318604.btc_tags.btc_address_high_fee`
)
,
btc_address_large_value_tx as (
    select 
        address,
        feature,
        null as name,
        case plots when '401' then 'P401' else plots end as plots,
        idx
    from 
        `chaingraph-318604.btc_tags.btc_address_large_value_tx`
)
,
btc_address_mimo as (
    select 
        *,
        1 as idx,
    from 
        `chaingraph-318604.btc_tags.btc_address_mimo`
)
,
btc_inactivate_addr as (
    select
        address,
        'inactive' as feature,
        null as name,
        'P117' as plots,
        4 as idx
    from
        `chaingraph-318604.btc_tags.btc_inactivate_addr`
)
,
btc_periodic_inactivate_addr as (
    select
        address,
        'periodic inactive' as feature,
        null as name,
        'P118' as plots,
        5 as idx
    from
        `chaingraph-318604.btc_tags.btc_priodical_inactivate_addr`
)
,
address_tag_union as (
    select * from btc_address_fast_tx
    union distinct
    select * from btc_address_high_fee
    union distinct
    select * from btc_address_large_value_tx
    union distinct
    select * from btc_address_mimo
    union distinct
    select * from btc_inactivate_addr
    union distinct
    select * from btc_periodic_inactivate_addr 
)
select * from address_tag_union 

