with 
dex_liquidity_provider as (
    select address from `chaingraph-318604.ethereum_tags.balancer_investor`
)
select 
    address,
    'Dex liquitity provider' as feature,
    null as name,
    'P701' as plots,
    1 as idx
from 
    dex_liquidity_provider