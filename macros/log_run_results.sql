{% macro log_run_results() %}
    {% if execute %}
        
        -- Create the logging table if it doesn't exist
        {% set create_table_sql %}
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dbt_run_results' AND schema_id = SCHEMA_ID('{{ target.schema }}'))
            BEGIN
                CREATE TABLE {{ target.schema }}.dbt_run_results (
                    run_id UNIQUEIDENTIFIER DEFAULT NEWID(),
                    invocation_id NVARCHAR(255),
                    run_started_at DATETIME2,
                    model_name NVARCHAR(255),
                    status NVARCHAR(50),
                    execution_time FLOAT,
                    rows_affected INT,
                    message NVARCHAR(MAX),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            END
        {% endset %}
        
        {% do run_query(create_table_sql) %}
        
        -- Insert run results
        {% for result in results %}
            {% set insert_sql %}
                INSERT INTO {{ target.schema }}.dbt_run_results 
                (invocation_id, run_started_at, model_name, status, execution_time, rows_affected, message)
                VALUES (
                    '{{ invocation_id }}',
                    '{{ run_started_at }}',
                    '{{ result.node.unique_id }}',
                    '{{ result.status }}',
                    {{ result.execution_time }},
                    {% if result.adapter_response.get('rows_affected') %}{{ result.adapter_response.rows_affected }}{% else %}NULL{% endif %},
                    '{{ result.message | replace("'", "''") }}'
                )
            {% endset %}
            
            {% do run_query(insert_sql) %}
        {% endfor %}
        
        {{ log("Run results logged to " ~ target.schema ~ ".dbt_run_results", info=True) }}
        
    {% endif %}
{% endmacro %}
