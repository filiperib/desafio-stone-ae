{{ config(
    materialized = 'table',
    schema='refined_dw'
) }}

with stg_tbl_transacoes as (
    select *
    from {{ref('stg_tbl_transacoes')}}
)

, cte_trata_null as (
    select 
        CASE 
            WHEN stg_tbl_transacoes.bd_cartao = '<null>'
                THEN null
            ELSE stg_tbl_transacoes.bd_cartao
        END                                             as bd_cartao
    from stg_tbl_transacoes

)

, source_data as (
    select distinct
        {{ gera_sk('cte_trata_null.bd_cartao') }}       as sk, 
        cte_trata_null.bd_cartao                        as ds_bnd_cartao,
        {{ data_local() }}                              as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as VARCHAR)        as usr_incl_reg
    from cte_trata_null
    where nullif(cte_trata_null.bd_cartao, '') is not null  -- Traz apenas o que não é nulo

    UNION

    SELECT 
        cast (-1 as varchar)                        as sk,
        'NÃO INFORMADO'                             as ds_bnd_cartao,
        {{ data_local() }}                          as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)    as usr_incl_reg
)

select *
from source_data