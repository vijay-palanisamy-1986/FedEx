{% macro get_web_app_field_device(user_agent) %}

   case
        when lower(user_agent) like '%android%' then 'Android'
        when lower({{get_web_app_field_device_raw('user_agent')}}) like '%android%' then 'Android'
        when lower({{get_web_app_field_device_raw('user_agent')}}) like '%iphone%' then 'iPhone'
        when lower({{get_web_app_field_device_raw('user_agent')}}) like '%ipad%' then 'iPad'
        when lower({{get_web_app_field_device_raw('user_agent')}}) like '%ipod%' then 'iPod'
        when lower({{get_web_app_field_device_raw('user_agent')}}) like '%macintosh%' then 'Macintosh'
        when lower({{get_web_app_field_device_raw('user_agent')}}) like '%linux%' then 'Linux'
        when lower({{get_web_app_field_device_raw('user_agent')}}) like '%windows%' then 'Windows'
        else 'Others'
    end

{% endmacro %}