{% macro get_web_field_browser_name(user_agent) %}

case
        when lower(user_agent) like '%chrome%' then 'Google Chrome'
        when lower(user_agent) like '%safari%' then 'Apple Safari'
        when lower(user_agent) like '%firefox%' then 'Mozilla Firefox'
        when lower(user_agent) like '%edge%' or lower(user_agent) like '%micro%' or lower(user_agent) like '%soft%' then 'Microsoft Edge'
        else 'Others'
      end

{% endmacro %}