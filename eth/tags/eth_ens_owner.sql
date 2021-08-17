-- delete from chaingraph-318604.ethereum_tags.ens_registor where 1 = 1
-- INSERT INTO chaingraph-318604.ethereum_tags.ens_registor

select 
    address,
    tx_timestamp,
    tag
from
(
    select
        a.address as old_address,
        b.address as address,
        b.tx_timestamp,
        b.tag
    from
    (
        select * from chaingraph-318604.ethereum_tags.ens_registor
    ) a
    right join
    (
        # v2 name register
        select 
            owner as address,
            min(block_timestamp) as tx_timestamp,
            'ens_registor' as tag
        from
        (
            SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController_event_NameRegistered`
            union all 
            SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController2_event_NameRegistered`
            union all 
            SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController3_event_NameRegistered`
        ) group by owner

        union all 

        select 
            owner as address,
            min(block_timestamp) as tx_timestamp,
            'ens_registor' as tag
        from
        (
            SELECT * FROM `blockchain-etl.ethereum_ens.Registrar0_event_HashRegistered`
        ) group by owner
    )b on a.address = b.address
) where old_address is null

-- union all 

-- select 
--     min(block_timestamp) as tx_timestamp,
--     bidder as address,
--     'ens_v1_bidder' as tag
-- from
-- (
--     SELECT * FROM `blockchain-etl.ethereum_ens.Registrar0_event_NewBid`
-- ) group by bidder

