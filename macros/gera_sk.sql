{% macro gera_sk(campo) %}
    {%- set sk = "md5(cast(" ~ campo ~ " as varchar))" -%}
    {{ return(sk) }}
{% endmacro %}
