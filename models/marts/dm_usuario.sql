{{ config(
    materialized = 'table',
    schema='refined_dw'
) }}

with stg_tbl_transacoes as (
    select *
    from {{ref('stg_tbl_transacoes')}}
)

, cte_ft_transacoes as (
    select
        cast(cd_usuario as varchar)                         as cd_usuario 
        , max(left(stg_tbl_transacoes.dt_hr_transacao, 10)) as dt_ultima_transacao
        , min(left(stg_tbl_transacoes.dt_hr_transacao, 10)) as dt_primeira_transacao
    from stg_tbl_transacoes
    group by cd_usuario
)

, source_data as (
    select distinct
        {{ gera_sk('cte_ft_transacoes.cd_usuario') }}     as sk,
        cd_usuario                                        as bk,  
        cte_ft_transacoes.dt_ultima_transacao             as dt_ultima_transacao,
        cte_ft_transacoes.dt_primeira_transacao           as dt_primeira_transacao,
        {{ data_local() }}                                as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)          as usr_incl_reg
    from cte_ft_transacoes
    where nullif(cte_ft_transacoes.cd_usuario, '') is not null  -- Traz apenas o que não é nulo

    UNION

    SELECT 
        cast (-1 as varchar)                        as sk,
        cast('-1' as varchar)                       as bk, 
        '1900-01-01'                                as dt_ultima_transacao,
        '1900-01-01'                                as dt_primeira_transacao,
        {{ data_local() }}                          as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)    as usr_incl_reg
)


select *
from source_data