with 
decentralised_options_user as (
    select address from `chaingraph-318604.ethereum_tags.opyn_user`
)
select 
    address,
    'Decentralised options user' as feature,
    null as name,
    'P701' as plots,
    5 as idx
from 
    decentralised_options_user