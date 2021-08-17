with
address_fast_tx as (
    select 
        address,
        feature,
        null as name,
        'P112' as plots,
        case idx 
            when 1 then 11 
            when 2 then 12 
            when 3 then 13 
        end as idx
    from 
        `chaingraph-318604.ethereum_tags.address_fast_tx`
)
,
address_high_fee as (
    select 
        address,
        feature,
        null as name,
        'P121' as plots,
        case idx
            when 11 then 10
            when 10 then 11
        end as idx
    from 
        `chaingraph-318604.ethereum_tags.address_high_fee`
)
,
address_large_value_tx as (
    select distinct 
        address,
        feature,
        null as name,
        plots,
        idx
    from 
        `chaingraph-318604.ethereum_tags.address_large_value_tx`
)
,
inactivate_addr as (
    select
        address,
        'inactive' as feature,
        null as name,
        'P117' as plots,
        4 as idx
    from
        `chaingraph-318604.ethereum_tags.eth_inactivate_addr`
)
,
periodic_inactivate_addr as (
    select
        address,
        'periodic inactive' as feature,
        null as name,
        'P118' as plots,
        5 as idx
    from
        `chaingraph-318604.ethereum_tags.eth_priodical_inactivate_addr`
)
,
address_tag_union as (
    select * from address_fast_tx
    union distinct
    select * from address_high_fee
    union distinct
    select * from address_large_value_tx
    union distinct
    select * from inactivate_addr
    union distinct
    select * from periodic_inactivate_addr 
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.P801-Others`
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.dex_trader`
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.dex_liquidity_provider`
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.decentralised_loaner`
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.decentralised_loan_depositor`
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.decentralised_flashloan_user`
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.decentralised_options_user`
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.decentralised_funds_user`
)
select * from address_tag_union 
;