with source_data as (
    select *
    from {{source('redshift_stone', 'tbl_transacoes')}}
)

select *
from source_data