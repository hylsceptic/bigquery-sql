insert into  chaingraph-318604.ethereum_tags.ens_owners

select 
    address,
    tx_timestamp,
    tag,
    name,
    expire_timestamp
from
(
    select
        a.address as old_address,
        b.address as address,
        b.tx_timestamp,
        b.tag,
        b.name,
        TIMESTAMP_SECONDS(cast(b.expire_timestamp as INT64)) as expire_timestamp
    from
    (
        select * from chaingraph-318604.ethereum_tags.ens_owners
    ) a
    right join
    (
        select
            address,
            tx_timestamp,
            'ens_owner' as tag,
            aa.name,
            coalesce(bb.expires, aa.expires) as expire_timestamp
        from 
        (
            select 
                block_timestamp as tx_timestamp,
                owner as address,
                name,
                expires
            from
            (
                SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController_event_NameRegistered`
                union all 
                SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController2_event_NameRegistered`
                union all 
                SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController3_event_NameRegistered`
            ) 
        ) aa
        left join 
        (
            select 
                name,
                max(expires) as expires
            from
            (
                SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController_event_NameRenewed`
                union all 
                SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController2_event_NameRenewed`
                union all 
                SELECT * FROM `blockchain-etl.ethereum_ens.ETHRegistrarController3_event_NameRenewed`
            ) group by name
        ) bb on aa.name =  bb.name
    ) b on a.address = b.address
) where old_address is null