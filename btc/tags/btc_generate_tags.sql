-- declare excute_day date;
-- set excute_day = date('2021-07-11');

-- truncate table `chaingraph-318604.btc_tags.generated_tags`;
insert into `chaingraph-318604.btc_tags.generated_tags` 

with
tx_inputs as
(
    SELECT 
        inputs.spent_transaction_hash,
        inputs.spent_output_index,
        `hash`,
        inputs.value,
        inputs.value / 100000000 * price as spent_usd_value,
        sum(inputs.value) over (partition by `hash`) as total_in,
        block_number as block_number,
        block_timestamp as block_timestamp,
        address,
        transactions.input_count
    FROM bigquery-public-data.crypto_bitcoin.transactions as transactions,
        transactions.inputs as inputs,
        unnest(inputs.addresses) as address,
        `chaingraph-318604.btc_data.btc_price`
    where inputs.type NOT IN ('nulldata', 'nonstandard')
        -- and block_timestamp_month = DATE_TRUNC(DATE_SUB(excute_day, INTERVAL 1 DAY), MONTH)
        -- and DATE(block_timestamp) = DATE_SUB(excute_day, INTERVAL 1 DAY)
        and date(block_timestamp) = date(price_timestamp)
),

tx_outputs as
(
    SELECT
        `hash`,
        outputs.index,
        outputs.value,
        (outputs.value / 100000000 * price) as receive_usd_value,
        sum(outputs.value) over (partition by `hash`) as total_out,
        block_number as block_number,
        block_timestamp as block_timestamp,
        address as address,
        transactions.output_count
    FROM bigquery-public-data.crypto_bitcoin.transactions as transactions,
        transactions.outputs as outputs,
        unnest(outputs.addresses) as address,
        `chaingraph-318604.btc_data.btc_price`
    where outputs.type NOT IN ('nulldata', 'nonstandard')
        -- and block_timestamp_month = DATE_TRUNC(DATE_SUB(excute_day, INTERVAL 1 DAY), MONTH)
        -- and DATE(block_timestamp) = DATE_SUB(excute_day, INTERVAL 1 DAY)
        and date(block_timestamp) = date(price_timestamp)
),

addr_total_spent_value as 
(
    select
        address,
        sum(value) as total_output,
        sum(spent_usd_value) as total_spent_usd
    from tx_inputs group by address
),

addr_total_receive_value as 
(
    select
        address,
        sum(value) as total_output,
        sum(receive_usd_value) as total_receive_usd
    from tx_outputs group by address
),

taged_transfers_input as 
(
    select * from tx_inputs a left join `chaingraph-318604.btc_tags.btc_address_tags_view` b using(address) where b.plots is not null
),

taged_transfers_output as 
(
    select * from tx_outputs a left join `chaingraph-318604.btc_tags.btc_address_tags_view` b using(address) where b.plots is not null
),

common_input as
(
    select
        `hash`,
        a.address,
        b.plots,
        b.ref_address,
        'common input' as relation,
        1 as idx,
        case when input_count <= 3 then 1 else 3 / input_count * base_score end as sub_weight
    from
        tx_inputs a left join
    (
        select distinct
            `hash`,
            plots,
            any_value(address) as ref_address,
            max(score) as base_score,
        from taged_transfers_input group by `hash`, plots
    ) b using(`hash`) where b.plots is not null and a.address != b.ref_address
),

common_output as
(
    select
        `hash`,
        a.address,
        b.plots,
        b.ref_address,
        'common output' as relation,
        2 as idx,
        case when output_count <= 3 then 1 else 3 / output_count * base_score end as sub_weight
    from
        tx_outputs a left join
        (
            select distinct  
                `hash`,
                plots,
                any_value(address) as ref_address,
                max(score) as base_score,
            from taged_transfers_output group by `hash`, plots
        ) b using(`hash`) where b.plots is not null and a.address != b.ref_address 
),

direct_source as
(
    select
        a.`hash`,
        a.address,
        a.spent_usd_value,
        a.spent_usd_value * (b.value / a.total_in) as source_usd_value, --input value allocated to the target output.
        a.value * (b.value / a.total_in) as source_value,
        b.plots,
        b.base_score,
        b.address as ref_address,
        a.spent_transaction_hash,
        a.spent_output_index
    from
        tx_inputs a 
    inner join 
        (
            select
                address,
                `hash`,
                `index`,
                any_value(value) as value,
                any_value(receive_usd_value) as receive_usd_value,
                plots,
                max(score) as base_score
            from
            taged_transfers_output group by address, `hash`, `index`, plots
        ) b
    on a.`hash` = b.`hash`
    where b.plots is not null and a.address != b.address and a.total_in > 0
),

direct_source_score as
(
    select 
        address,
        plots,
        'direct source' as relation,
        3 as idx,
        sum(total_usd_value / total_spent_usd * base_score * 
            (case when total_usd_value > 10000 then 1 else total_usd_value / 10000 end)) as sub_weight
    from 
    (
        SELECT 
            address,
            plots,
            -- sum(value) as total_value,
            sum(source_usd_value) as total_usd_value,
            max(base_score) as base_score
        FROM (
            select 
                `hash`,
                address,
                plots,
                -- any_value(value) as value,
                any_value(source_usd_value) as source_usd_value,
                max(base_score) as base_score
            from
                direct_source group by `hash`, address, plots
         ) group by address, plots
    ) a left join addr_total_spent_value b using(address)
    where a.total_usd_value > 0
    group by address, plots
),

direct_dest as
(
    select
        a.`hash`,
        a.address,
        a.value as value,
        a.total_out,
        a.value * (b.value / b.total_in) as dest_value,
        a.receive_usd_value,
        b.value as value_out,
        b.base_score,
        b.plots,
        b.address as ref_address,
        a.index
    from
        tx_outputs a 
    inner join 
        (
            select distinct 
                address,
                any_value(total_in) as total_in,
                `hash`,
                any_value(value) as value,
                plots,
                max(score) as base_score
            from
            taged_transfers_input group by address, `hash`, plots
        ) b 
    on a.`hash` = b.`hash`
    where b.plots is not null and a.address != b.address  
),

direct_dest_score as
(
    select 
        address,
        plots,
        'direct dest' as relation,
        4 as idx,
        max(total_usd_value / total_receive_usd * base_score * (case when total_usd_value > 10000 then 1 else total_usd_value / 10000 end)) as sub_weight
    from 
    (
        SELECT 
            address,
            plots,
            sum(usd_value) as total_usd_value,
            max(base_score) as base_score
        FROM (
            select 
                `hash`,
                address,
                plots,
                any_value(receive_usd_value) as usd_value,
                max(base_score) as base_score
            from
                direct_dest group by `hash`, address, plots
        ) group by address, plots
    ) a left join addr_total_receive_value b using(address)
    where total_usd_value > 0
    group by address, plots
),

indirect_source as
(
    select
        a.address,
        a.hash,
        b.ref_address,
        b.ref_address2,
        a.value,
        a.spent_usd_value,
        b.source_value as ref_source_value,
        a.total_in,
        a.spent_usd_value * b.source_value / a.total_in as source_usd_value, --spent value allocated to the target.
        b.plots,
        b.base_score
    from 
        tx_inputs a 
    left join 
    (
        select distinct
            a.ref_address,
            a.address as ref_address2,
            a.plots,
            a.source_value,
            b.hash,
            a.base_score
        from 
        direct_source a right join tx_outputs b on a.spent_transaction_hash = b.hash and a.spent_output_index = b.index 
        where a.spent_transaction_hash is not null and a.spent_output_index is not null
    ) b on a.hash = b.hash where b.hash is not null and a.address != b.ref_address and a.total_in > 0
),

indirect_source_score as
(
    select 
        address,
        plots,
        5 as idx,
        'indirect source' as relation,
        sum(total_usd_value / total_spent_usd * base_score *
            (case when total_usd_value > 10000 then 1 else total_usd_value / 10000 end)) as sub_weight,
    from 
    (
        SELECT 
            address,
            plots,
            -- sum(value) as total_value,
            sum(source_usd_value) as total_usd_value,
            max(base_score) as base_score
        FROM (
            select
                `hash`,
                address,
                plots,
                any_value(source_usd_value) as source_usd_value,
                max(base_score) as base_score
            from
                indirect_source group by `hash`, address, plots
         ) group by address, plots
    ) a left join addr_total_spent_value b using(address)
    where a.total_usd_value > 0
    group by address, plots
),

indirect_dest as
(
    select
        a.address,
        a.hash,
        b.ref_address,
        b.ref_address2,
        a.value,
        a.receive_usd_value,
        b.total_in,
        b.dest_value as ref_dest_value,
        a.receive_usd_value * b.dest_value / b.total_in as dest_usd_value, --receive value from target.
        b.plots,
        base_score 
    from 
        tx_outputs a 
    left join 
    (
        select distinct
            a.ref_address,
            a.address as ref_address2,
            a.plots,
            a.dest_value,
            b.hash,
            b.total_in,
            a.base_score
        from 
        direct_dest a right join tx_inputs b 
        on a.hash = b.spent_transaction_hash 
            and a.index = b.spent_output_index 
        where a.address is not null and a.index is not null
    ) b on a.hash = b.hash where b.hash is not null and  a.address != b.ref_address and a.total_out > 0
),

indirect_dest_score as
(
    select 
        address,
        plots,
        6 as idx,
        'indirect dest' as relation,
        sum(total_usd_value / total_receive_usd * base_score *
            (case when total_usd_value > 10000 then 1 else total_usd_value / 10000 end)) as sub_weight,
    from 
    (
        SELECT 
            address,
            plots,
            sum(dest_usd_value) as total_usd_value,
            max(base_score) as base_score
        FROM (
            select
                `hash`,
                address,
                plots,
                any_value(dest_usd_value) as dest_usd_value,
                max(base_score) as base_score
            from
                indirect_dest group by `hash`, address, plots
         ) group by address, plots
    ) a left join addr_total_receive_value b using(address) 
    where a.total_usd_value > 0 
    group by address, plots
),

tags as
(
        select distinct 
            address,
            plots,
            'direct' as relation,
            0 as idx,
            1 as sub_weight
        from `chaingraph-318604.btc_tags.btc_address_tags`
    union all 
        select distinct
            address,
            plots,
            relation,
            idx,
            sub_weight
        from common_input
    union all 
        select distinct
            address,
            plots,
            relation,
            idx,
            sub_weight
        from common_output
    union all 
        select distinct
            address,
            plots,
            relation,
            idx,
            sub_weight
        from direct_source_score
    union all 
        select distinct
            address,
            plots,
            relation,
            idx,
            sub_weight
        from direct_dest_score
    union all 
        select distinct
            address,
            plots,
            relation,
            idx,
            sub_weight
        from indirect_source_score
    union all 
        select distinct
            address,
            plots,
            relation,
            idx,
            sub_weight
        from indirect_dest_score
)

select * from tags

-- select * from addr_total_output 

