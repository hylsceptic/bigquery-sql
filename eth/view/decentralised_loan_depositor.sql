with 
decentralised_loan_depositor as (
    select address from `chaingraph-318604.ethereum_tags.aave_depositor`
    union distinct 
    select address from `chaingraph-318604.ethereum_tags.compound_supplier`
)
select 
    address,
    'Decentralised loan depositor' as feature,
    null as name,
    'P701' as plots,
    3 as idx
from 
    decentralised_loan_depositor