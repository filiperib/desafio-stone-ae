{{ config(
    materialized = 'table',
    schema='refined_dw'
) }}

with stg_tbl_transacoes as (
    select *
    from {{ref('stg_tbl_transacoes')}}
)

, source_data as (
    select distinct
        {{ gera_sk('stg_tbl_transacoes.mtd_captura') }}     as sk,
        stg_tbl_transacoes.mtd_captura                      as ds_mtd_captura,
        {{ data_local() }}                                  as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)            as usr_incl_reg
        
    from stg_tbl_transacoes
    where nullif(stg_tbl_transacoes.mtd_captura, '') is not null  -- Traz apenas o que não é nulo

    UNION

    SELECT 
        cast (-1 as varchar)                        as sk,
        'NÃO INFORMADO'                             as ds_mtd_captura,
        {{ data_local() }}                          as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)    as usr_incl_reg
)


select *
from source_data