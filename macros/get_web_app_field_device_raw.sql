{% macro get_web_app_field_device_raw(user_agent) %}

   substr(user_Agent, 14, position( ';' IN user_agent) - 14)

{% endmacro %}