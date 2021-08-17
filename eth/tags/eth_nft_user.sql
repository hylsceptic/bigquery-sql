create or replace table `chaingraph-318604.ethereum_tags.nft_user` as
with 
erc721_addr as (
    select 
        address 
    from 
        `bigquery-public-data.crypto_ethereum.contracts`
    where true
        and is_erc721 is true
)
,
transfers as (
    SELECT 
        token_address,
        from_address,
        to_address,
    FROM 
        `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE true 
        and date(block_timestamp) = date_sub(@run_date, interval 1 day)
        -- and DATE(block_timestamp) <= "2021-08-10"
)
,
erc721_transfers as (
    select 
        from_address,
        to_address,
    from
    (
        transfers a
        join 
        erc721_addr b
        on a.token_address = b.address
    )
)
,
erc721_addresses as (
    select from_address as address from erc721_transfers 
    union distinct 
    select to_address as address from erc721_transfers 
)
,
nft_user as (
    select distinct 
        address,
        'NFT user' as feature,
        null as name,
        'P801' as plots,
        2 as idx
    from 
        erc721_addresses
)
-- select * from nft_user
select 
    *
from
(
    select * from `chaingraph-318604.ethereum_tags.nft_user`
    union distinct 
    select * from nft_user
)
;