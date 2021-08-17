with 
decentralised_funds_user as (
    select address from `chaingraph-318604.ethereum_tags.yearn_user`
)
select 
    address,
    'Decentralised funds user' as feature,
    null as name,
    'P701' as plots,
    6 as idx
from 
    decentralised_funds_user