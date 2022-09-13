{# 
{%- macro delta_apply_macro(target_relation,temp_relation,primary_key) -%}
    
    {{ create_table_as(false, temp_relation, sql) }}
    
    {%- set columns = (adapter.get_columns_in_relation(temp_relation)) -%}
    {%- set csv_columns = get_quoted_csv(columns | map(attribute="name")) -%}
    {%- set column_names=csv_columns.split(',')-%}
    
    
    
    CREATE OR REPLACE
    TABLE "TEMP"."ACCOUNT_TEST_TGT_CDC" AS (
    SELECT
        {% for col in column_names %}
            CASE
                WHEN {{temp_relation}}.{{'ETL_Row_Deleted_Flag'}} = 'Y' THEN {{target_relation}}.{{col}}
                ELSE {{temp_relation}}.{{col}}
            END AS {{col}}{{','}}
          
        {% endfor %}
        END AS ETL_New_Row_Flag,
        
        CASE
            WHEN src.ETL_Row_Deleted_Flag = 'Y'
            AND tgt.ETL_Row_Deleted_Flag = FALSE THEN 'Y'
            ELSE 'N'
        END AS ETL_Row_Deleted_Flag,
        CASE
            WHEN src."ID" IS NOT NULL
            AND tgt.ETL_Row_Deleted_Flag = TRUE
            AND tgt.ETL_Row_Current_Flag = TRUE THEN 'Y'
            WHEN ((src."ACCOUNT_NUMBER" = tgt."ACCOUNT_NUMBER")
            OR (src."ACCOUNT_NUMBER" IS NULL
            AND tgt."ACCOUNT_NUMBER" IS NULL))
            AND ((src."TYPE" = tgt."TYPE")
            OR (src."TYPE" IS NULL
            AND tgt."TYPE" IS NULL))
            AND ((src."NAME" = tgt."NAME")
            OR (src."NAME" IS NULL
            AND tgt."NAME" IS NULL)) THEN 'N'
            WHEN tgt."ID" IS NULL THEN 'N'
            WHEN src.ETL_Row_Deleted_Flag = 'Y' THEN 'N'
            ELSE 'Y'
        END AS ETL_Row_Updated_Flag,
        CURRENT_TIMESTAMP AS ETL_Row_Effective_Date
    FROM
        "TEMP"."ACCOUNT_TEST_C_STG" src
    LEFT JOIN "TEMP"."ACCOUNT_TEST_TGT" tgt ON
        ((src."ID" = tgt."ID")
            OR (src."ID" IS NULL
                AND tgt."ID" IS NULL))
    WHERE
        (tgt."ID" IS NULL)
        OR tgt.ETL_Row_Current_Flag = TRUE)

        
{%- endmacro -%}



 #}
