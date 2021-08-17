with 
decentralised_flashloan_user as (
    select address from `chaingraph-318604.ethereum_tags.aave_flashloan_user`
    union distinct 
    select address from `chaingraph-318604.ethereum_tags.balancer_flashloan_user`
)
select 
    address,
    'Decentralised loan depositor' as feature,
    null as name,
    'P701' as plots,
    3 as idx
from 
    decentralised_flashloan_user