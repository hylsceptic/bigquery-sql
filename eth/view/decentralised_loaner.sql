with 
decentralised_loaner as (
    select address from `chaingraph-318604.ethereum_tags.aave_borrower`
    union distinct 
    select address from `chaingraph-318604.ethereum_tags.compound_borrower`
    union distinct 
    select address from `chaingraph-318604.ethereum_tags.dydx_user`
)
select 
    address,
    'Decentralised loaner' as feature,
    null as name,
    'P701' as plots,
    2 as idx
from 
    decentralised_loaner