# delete from `chaingraph-318604.ethereum_tags.compound_borrower` where 1=1
# insert into `chaingraph-318604.ethereum_tags.compound_borrower`
with
tx_subset as
(
    select 
        block_timestamp,
        `hash` as transaction_hash,
        from_address
    from
        `bigquery-public-data.crypto_ethereum.transactions`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cBAT as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cBAT_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cCOMP as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cCOMP_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cDAI as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cDAI_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cETH as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cETH_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cLINK as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cLINK_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cREP as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cBAT_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cSAI as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cSAI_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cTUSD as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cTUSD_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cUNI as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cUNI_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cUSDT as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cUSDT_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cWBTC2 as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cWBTC2_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cWBTC as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cWBTC_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
cZRX as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_compound.cZRX_event_Borrow`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
union_set as
(
    select * from cBAT  
    union distinct 
    select * from cCOMP  
    union distinct 
    select * from cDAI 
    union distinct 
    select * from cETH 
    union distinct 
    select * from cLINK
    union distinct 
    select * from cREP
    union distinct 
    select * from cSAI 
    union distinct 
    select * from cTUSD 
    union distinct 
    select * from cUNI
    union distinct 
    select * from cUSDT
    union distinct 
    select * from cWBTC2 
    union distinct 
    select * from cWBTC
    union distinct 
    select * from cZRX
)
,
tmp_addr_ts_tag as
(
    select 
        a.from_address as address,
        min(a.block_timestamp) as tx_timestamp,
        'Compound borrower' as tag
    from
    (
        tx_subset a
        join
        union_set b
        on a.transaction_hash = b.transaction_hash
    )
    group by address
)
select
    address_a as address,
    tx_timestamp,
    tag
from
(
    select
        a.address as address_a,
        b.address as address_b,
        a.tx_timestamp as tx_timestamp,
        a.tag as tag
    from 
    (
        tmp_addr_ts_tag a
        left join 
        `chaingraph-318604.ethereum_tags.compound_borrower` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Get historic data
# -------------------------- #
# select distinct * from tmp_addr_ts_tag
;