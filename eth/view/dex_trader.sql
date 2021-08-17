with 
dex_trader as (
    select address from `chaingraph-318604.ethereum_tags.uniswap_trader`
    union distinct 
    select address from `chaingraph-318604.ethereum_tags.balancer_trader`
    union distinct 
    select address from `chaingraph-318604.ethereum_tags.curve_trader`
)
select 
    address,
    'Dex trader' as feature,
    null as name,
    'P701' as plots,
    0 as idx
from 
    dex_trader