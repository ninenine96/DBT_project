{{macro materialize()}}

    {% set sql %}
        select {{this.table}} as details
    {% endset %}

{{endmacro}}