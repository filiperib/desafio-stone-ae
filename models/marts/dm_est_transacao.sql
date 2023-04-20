{{ config(
    materialized = 'table',
    schema='refined_dw'
) }}

with stg_tbl_transacoes as (
    select *
    from {{ref('stg_tbl_transacoes')}}
)

, stg_trata_sk as (
    select *
        , upper (stg_tbl_transacoes.est_transacao) as est_transacao_sk
    from {{ref('stg_tbl_transacoes')}}
)

, source_data as (
    select distinct
        {{ gera_sk('stg_trata_sk.est_transacao_sk') }}      as sk,
        upper (stg_trata_sk.est_transacao)                  as ds_est_transacao,
        {{ data_local() }}                                  as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)            as usr_incl_reg
    from stg_trata_sk
    where nullif(stg_trata_sk.est_transacao, '') is not null  -- Traz apenas o que não é nulo

    UNION

    SELECT 
        cast (-1 as varchar)                                    as sk,
        'NÃO INFORMADO'                                         as ds_est_transacao,
        {{ data_local() }}                                      as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)                as usr_incl_reg
)


select *
from source_data