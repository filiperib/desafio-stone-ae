{{ config(
    materialized = 'table',
    schema='refined_dw'
) }}

with stg_tbl_transacoes as (
    select *
    from {{ref('stg_tbl_transacoes')}}
)

, cte_stg as (
    select *
        , stg_tbl_transacoes.mun_usuario || stg_tbl_transacoes.uf_usuario as concat_sk
    from stg_tbl_transacoes
)

, source_data as (
    select distinct
        {{ gera_sk('concat_sk') }}                          as sk,
        cte_stg.mun_usuario                                 as ds_municipio, 
        cte_stg.uf_usuario                                  as ds_uf,
        {{ data_local() }}                                  as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)            as usr_incl_reg
    from cte_stg
    where nullif(cte_stg.mun_usuario, '') is not null  -- Traz apenas o que não é nulo
    and nullif(cte_stg.uf_usuario, '') is not null  -- Traz apenas o que não é nulo

    UNION

    SELECT 
        cast (-1 as varchar)                        as sk,
        'NÃO INFORMADO'                             as ds_municipio,
        'NÃO INFORMADO'                             as ds_uf,
        {{ data_local() }}                          as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)    as usr_incl_reg

)

select *
from source_data