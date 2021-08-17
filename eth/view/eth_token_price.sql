select 
    address,
    date(`timestamp`) as price_day,
    any_value(price) as price,
    any_value(symbol) as symbol
from
(
    SELECT * FROM `chaingraph-318604.ethereum_data.history_token_prices`
union all 
    select * from `chaingraph-318604.ethereum_data.token_prices`
) group by address, date(`timestamp`)