{{ config(
    materialized = 'table',
    schema='refined_dw'
) }}

with stg_dm_tempo AS (
    SELECT DATE(date('{{ var("start_date") }}') + (n || ' days')::interval) AS dt_data
        FROM (
                SELECT ROW_NUMBER() OVER () - 1 AS n 
                FROM (
                    SELECT 1
                    FROM stl_connection_log
                    limit CAST ({{ var("forecast_years") }} * 365 AS INTEGER)
                ) 
    ) 
)

, cte_dm_tempo AS (
  SELECT dt_data
    ,EXTRACT(DOW FROM dt_data) AS nr_dia_sem_dt_data
    ,DATEADD(month, -12, dt_data) AS dt_data_mdm_aa
    ,DATEADD(month, 12, dt_data) AS dt_data_mdm_ap
    ,EXTRACT(DOW FROM DATEADD(month, -12, dt_data)) AS nr_dia_sem_dt_data_mdm_aa
    ,EXTRACT(DOW FROM DATEADD(month, 12, dt_data)) AS nr_dia_sem_dt_data_mdm_ap
  FROM stg_dm_tempo
)

, cte_dm_tempo_extended AS (
  SELECT dt_data
    ,dt_data_mdm_aa
    ,CASE 
      WHEN (EXTRACT(month FROM dt_data) = 12 AND EXTRACT(day FROM dt_data) IN (24, 25, 31))
        OR (EXTRACT(month FROM dt_data) = 1 AND EXTRACT(day FROM dt_data) = 1)
      THEN dt_data_mdm_aa
      ELSE DATEADD(day
                  ,CASE 
                    WHEN nr_dia_sem_dt_data > nr_dia_sem_dt_data_mdm_aa
                      THEN nr_dia_sem_dt_data - nr_dia_sem_dt_data_mdm_aa
                    ELSE nr_dia_sem_dt_data - nr_dia_sem_dt_data_mdm_aa + 7
                  END
                  ,dt_data_mdm_aa)
    END AS dt_data_mds_aa
    ,dt_data_mdm_ap
    ,CASE 
      WHEN (EXTRACT(month FROM dt_data) = 12 AND EXTRACT(day FROM dt_data) IN (24, 25, 31))
        OR (EXTRACT(month FROM dt_data) = 1 AND EXTRACT(day FROM dt_data) = 1)
      THEN dt_data_mdm_ap
      ELSE DATEADD(day
                  ,CASE 
                    WHEN nr_dia_sem_dt_data_mdm_ap > nr_dia_sem_dt_data
                      THEN nr_dia_sem_dt_data - nr_dia_sem_dt_data_mdm_ap
                    ELSE nr_dia_sem_dt_data - nr_dia_sem_dt_data_mdm_ap - 7
                  END
                  ,dt_data_mdm_ap)
    END AS dt_data_mds_ap
  FROM cte_dm_tempo
)

, source_data AS (
  SELECT TO_CHAR(dt_data, 'YYYYMMDD') AS sk
        ,dt_data
        ,dt_data_mdm_aa
        ,dt_data_mds_aa
        ,dt_data_mdm_ap
        ,dt_data_mds_ap
        ,DATE_TRUNC('month', dt_data)::DATE AS dt_prim_dia_mes
        ,LAST_DAY(DATE_TRUNC('month', dt_data))::DATE AS dt_ult_dia_mes
        ,TO_CHAR(dt_data, 'YYYYMM') AS ds_ano_mes
        ,EXTRACT(year FROM dt_data) AS nr_ano
        ,EXTRACT(month FROM dt_data) AS nr_mes
        ,EXTRACT(day FROM dt_data) AS nr_dia
        ,EXTRACT(week FROM dt_data) AS nr
        ,    DATE_PART(w, dt_data) AS nr_semana,
    CASE DATE_PART(dw, dt_data)
        WHEN 0 THEN 'Domingo'
        WHEN 1 THEN 'Segunda'
        WHEN 2 THEN 'Terça'
        WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta'
        WHEN 5 THEN 'Sexta'
        WHEN 6 THEN 'Sábado'
    END AS ds_dia_semana,
    CASE DATE_PART(dw, dt_data)
        WHEN 0 THEN 'Dom'
        WHEN 1 THEN 'Seg'
        WHEN 2 THEN 'Ter'
        WHEN 3 THEN 'Qua'
        WHEN 4 THEN 'Qui'
        WHEN 5 THEN 'Sex'
        WHEN 6 THEN 'Sab'
    END AS ds_dia_semana_abreviado,
    CASE DATE_PART(month, dt_data)
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS ds_mes,
    CASE DATE_PART(month, dt_data)
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Fev'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Abr'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul'
        WHEN 8 THEN 'Ago'
        WHEN 9 THEN 'Set'
        WHEN 10 THEN 'Out'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Dez'
    END AS ds_mes_abreviado,
    'Q' || CAST(DATE_PART(qtr, dt_data) AS VARCHAR) AS ds_trimestre,
    CASE 
        WHEN DATE_PART(qtr, dt_data) IN (1,2) THEN 'S1'
        ELSE 'S2'
    END AS ds_semestre,
    DATE_PART(dw, dt_data) IN (1,7) AS fl_fim_semana,
        {{ data_local() }}                        as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar)  as usr_incl_reg 
    FROM cte_dm_tempo_extended

    UNION

    SELECT 
        cast(-1 as varchar) 			as sk,
        cast('1900-01-01' as date) 		as dt_data,
        cast('1900-01-01' as date) 		as dt_data_mdm_aa,
        cast('1900-01-01' as date) 		as dt_data_mds_aa,
        cast('1900-01-01' as date) 		as dt_data_mdm_ap,
        cast('1900-01-01' as date) 		as dt_data_mds_ap,
        cast('1900-01-01' as date) 		as dt_prim_dia_mes,
        cast('1900-01-01' as date) 		as dt_ult_dia_mes,
        cast(-1 as varchar)				as ds_ano_mes,
        0				    			as nr_ano,
        0				    			as nr_mes,
        0				    			as nr_dia,
        0	    						as nr,
        0   			    			as nr_semana,
        cast(-1 as varchar)    			as ds_dia_semana,
        cast(-1 as varchar)				as ds_dia_semana_abreviado,
        cast(-1 as varchar) 			as ds_mes,
        cast(-1 as varchar)				as ds_mes_abreviado,
        cast(-1 as varchar)				as ds_trimestre,
        cast(-1 as varchar)				as ds_semestre,
        false 							as fl_fim_semana,
        {{ data_local() }}  			as dt_incl_reg, 
        cast('{{ var("dbt_user") }}' as varchar) as usr_incl_reg  
)

select * from source_data
