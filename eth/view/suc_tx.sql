select 
    `hash`
from
    (
        (
            select
                `hash`
            from 
                `bigquery-public-data.crypto_ethereum.transactions` 
            where receipt_status = 1
        )
        union distinct 
        (
            select 
                transaction_hash
            from
                `chaingraph-318604.ethereum_metrics.tx_before_byzantium`
            where receipt_status = 1
        )
    )