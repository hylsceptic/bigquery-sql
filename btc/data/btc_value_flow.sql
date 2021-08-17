delete from chaingraph-318604.btc_data.btc_value_flow where date(partition_date) = DATE_SUB(@run_date, INTERVAL 1 DAY);

insert into chaingraph-318604.btc_data.btc_value_flow

select 
    a.*,
    a.in_value * b.price as in_usd_value,
    a.out_value * b.price as out_usd_value,
    a.net_value * b.price as net_usd_value,
    case when date(a.block_timestamp) < '2016-01-01' then DATE_TRUNC(date(a.block_timestamp), MONTH) else date(a.block_timestamp) end as partition_date
from 
    (
        select 
            case when a.`hash` is not null then a.`hash` else b.hash end as `hash`,
            case when a.block_number is not null then a.block_number else b.block_number end as block_number,
            case when a.address is not null then a.address else b.address end as address,
            case when a.block_timestamp is not null then a.block_timestamp else b.block_timestamp end as block_timestamp,
            b.in_value,
            a.out_value,
            case when out_value is null then 0 else out_value end  - (case when in_value is null then 0 else in_value end) as net_value
        from
        (
            SELECT 
                `hash`,
                max(block_number) as block_number,
                max(block_timestamp) as block_timestamp,
                sum(outputs.value / array_length(outputs.addresses)) * pow(10, -8) as out_value,
                single_output_address as address
            FROM bigquery-public-data.crypto_bitcoin.transactions as transactions,
                transactions.outputs as outputs,
                unnest(outputs.addresses) as single_output_address
            where outputs.type NOT IN ('nulldata', 'nonstandard')
            -- and date(block_timestamp_month) = '2017-05-01'
                and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
                and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
            group by `hash`, address
        ) a
        full outer join 
        (
            SELECT 
                `hash`,
                max(block_number) as block_number,
                max(block_timestamp) as block_timestamp,
                sum(inputs.value / array_length(inputs.addresses)) * pow(10, -8) as in_value,
                single_input_address as address
            FROM bigquery-public-data.crypto_bitcoin.transactions as transactions,
                transactions.inputs as inputs,
                unnest(inputs.addresses) as single_input_address
            where inputs.type NOT IN ('nulldata', 'nonstandard')
                and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
                and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
            group by `hash`, address
        ) b on a.`hash` = b.`hash` and a.address = b.address
    ) a left join `chaingraph-318604.btc_data.btc_price` b on date(block_timestamp) = date(b.price_timestamp)

-- table schama
-- hash:STRING,block_number:INTEGER,address:STRING,block_timestamp:TIMESTAMP,in_value:FLOAT,out_value:FLOAT,net_value:FLOAT,in_usd_value:FLOAT,out_usd_value:FLOAT,net_usd_value:FLOAT,block_timestamp_month:DATE
