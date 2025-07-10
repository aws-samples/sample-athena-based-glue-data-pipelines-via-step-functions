{% from "audit.jinja2" import select_grain_cols,add_join  %}
INSERT INTO {{ audit.param_audit_db }}.{{ audit.param_audit_table}}
WITH input_dates AS (
    SELECT
    CAST('{{ globals.param_exec_date }}' AS date) AS exec_date,
    CAST('{{ globals.param_exec_date }}' AS date) AS curr_period,
    MAX(exec_date) AS prev_period
    FROM {{ globals.param_processed_db_name }}.{{ globals.param_monthly_results_table_name }}
    WHERE exec_date < CAST('{{ globals.param_exec_date }}' AS date)
    )
{%- for config in configs %}
    SELECT
    '{{ globals.param_layer }}' AS layer,

    {%- set tables_with_table_level_filters = [] %}
    {%- for table in table_level_where %}
        {{- tables_with_table_level_filters.append(table['table_name']) or "" -}}
        {% if globals.param_audited_table_name == table['table_name'] %}
                {{ table['attribute'] }} AS attribute,
        {% endif %}
    {%- endfor %}

    {%- if globals.param_audited_table_name not in tables_with_table_level_filters %}
        '{{ config.render_params.param_audited_attribute }}' AS attribute,
    {%- endif %}

    {{ select_grain_cols(config.render_params.select_config,['']) }},
    {%- if globals.param_audited_table_name in table_select_period_pattern['pattern_1']  %}
        CAST((substr(b.{{ globals.param_month_column_name }},1,4)||'-'||substr(b.{{ globals.param_month_column_name }},5,7)||'-01') AS date) AS period,
    {%- elif globals.param_audited_table_name in table_select_period_pattern['pattern_2']  %}
        {{ globals.param_month_column_name }} AS period,
    {%- elif globals.param_audited_table_name in table_select_period_pattern['pattern_3']  %}
        date({{ globals.param_month_column_name }}) AS period,
    {%- elif globals.param_audited_table_name in table_select_period_pattern['pattern_4']  %}
        CAST((b.{{ globals.param_month_column_name }}||'-'||'01-01') as date) AS period,
    {%- endif %}

    CAST(b.{{ config.render_params.param_audited_attribute }} AS VARCHAR) AS curr_value,
    CAST(a.{{ config.render_params.param_audited_attribute }} AS VARCHAR) AS prev_value,
    '{{ globals.param_pipeline_name }}' AS pipeline,
    b.exec_date AS exec_date,
    '{{ globals.param_audited_table_name }}' AS table_name,
    '{{ globals.param_grain }}' AS time_grain
    FROM
    {{ globals.param_layer }}_db_{{ globals.param_stage }}.{{ globals.param_audited_table_name }}  a
    {{ add_join(config.render_params.join_config) }}
    WHERE
    a.{{ config.render_params.param_audited_attribute }} <> b.{{ config.render_params.param_audited_attribute }}
    AND a.exec_date= ( select prev_period from input_dates )
    AND b.exec_date= ( select curr_period from input_dates )
    {%- for table in table_level_where -%}
        {%- if globals.param_audited_table_name == table['table_name'] -%}
            {%- for condition in table['conditions'] %}
                {{ condition['condition'] }}
            {%- endfor %}
        {%- endif -%}
    {%- endfor %}
    {% if not loop.last -%}
    UNION
    {%- endif -%}
{%- endfor %}
