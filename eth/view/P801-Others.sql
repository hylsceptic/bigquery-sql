with 
contract_deployer as (
    select 
        address,
        'Contract deployer' as feature,
        null as name,
        'P801' as plots,
        0 as idx
    from `chaingraph-318604.ethereum_tags.contract_deployer`
)
,
eth2_staker as (
    select 
        address,
        'ETH2.0 staker' as feature,
        null as name,
        'P801' as plots,
        1 as idx
    from 
        `chaingraph-318604.ethereum_tags.addr_eth2`
)
,
addr_tags as (
    select * from contract_deployer
    union distinct
    select * from eth2_staker
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.nft_user`
)
select 
    *
from 
    addr_tags