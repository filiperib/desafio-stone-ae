{{ config(
    materialized = 'table',
    schema='refined_dw'
) }}


with stg_tbl_transacoes as (
    select *
    from {{ref('stg_tbl_transacoes')}}
)

, lkp_dm_tempo as (
    select *
    from {{ref('dm_tempo')}}
)

, lkp_dm_bnd_cartao as (
    select *
    from {{ref('dm_bnd_cartao')}}
)

, lkp_dm_est_transacao as (
    select *
    from {{ref('dm_est_transacao')}}
)

, lkp_dm_loc_geografica as (
    select *
    from {{ref('dm_loc_geografica')}}
)

, lkp_dm_mtd_captura as (
    select *
    from {{ref('dm_mtd_captura')}}
)

, lkp_dm_mtd_pagamento as (
    select *
    from {{ref('dm_mtd_pagamento')}}
)

, lkp_dm_usuario as (
    select *
    from {{ref('dm_usuario')}}
)

-- Cria ranking ordenando por data/hora da transacação (mais recente) pela chave transacao/usuario
, cte_ft_transacoes as (
    select *
    , stg_tbl_transacoes.mun_usuario || stg_tbl_transacoes.uf_usuario                         as concat_sk
    , row_number() over (partition by cd_usuario, cd_transacao order by dt_hr_transacao desc) as ord 
    from stg_tbl_transacoes
)


, source_data as (
    select 
        cte_ft_transacoes.cd_transacao                                  as cd_transacao
        , coalesce(lkp_dm_tempo.sk, '-1')                               as dm_tempo_sk
        , coalesce(lkp_dm_bnd_cartao.sk, '-1')                          as dm_bnd_cartao_sk
        , coalesce(lkp_dm_est_transacao.sk, '-1')                       as dm_est_transacao_sk
        , coalesce(lkp_dm_loc_geografica.sk, '-1')                      as dm_loc_geografica_sk
        , coalesce(lkp_dm_mtd_captura.sk, '-1')                         as dm_mtd_captura_sk
        , coalesce(lkp_dm_mtd_pagamento.sk, '-1')                       as dm_mtd_pagamento_sk
        , right(cte_ft_transacoes.dt_hr_transacao, 12)                  as hr_transacao
        , cast (cte_ft_transacoes.vlr_transacao as double precision)    as vl_transacao
        , coalesce(lkp_dm_usuario.sk, '-1')                             as dm_usuario_sk
    from cte_ft_transacoes
    left join lkp_dm_bnd_cartao
        on lkp_dm_bnd_cartao.sk = {{ gera_sk('cte_ft_transacoes.bd_cartao') }}
    left join lkp_dm_est_transacao
        on lkp_dm_est_transacao.sk = {{ gera_sk('cte_ft_transacoes.est_transacao') }}
    left join lkp_dm_loc_geografica
        on lkp_dm_loc_geografica.sk = {{ gera_sk('concat_sk') }}
    left join lkp_dm_mtd_captura
        on lkp_dm_mtd_captura.sk = {{ gera_sk('cte_ft_transacoes.mtd_captura') }}
    left join lkp_dm_mtd_pagamento
        on lkp_dm_mtd_pagamento.sk = {{ gera_sk('cte_ft_transacoes.mtd_pagamento') }}
    left join lkp_dm_tempo
        on lkp_dm_tempo.sk = regexp_replace(left(cte_ft_transacoes.dt_hr_transacao,10), '[^0-9]', '')
    left join lkp_dm_usuario
        on lkp_dm_usuario.sk = {{ gera_sk('cte_ft_transacoes.cd_usuario') }}
    where cte_ft_transacoes.ord = 1 -- Traz a transação mais recente da chave usuario/transacao
)

select * 
from source_data