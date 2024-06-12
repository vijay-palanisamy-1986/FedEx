{% macro get_web_field_page_type(page_name) %}

    case
        when lower(page_name) like '%home%' then 'Home'
        when lower(page_name) like '%login%' then 'Login/Logout'
        when lower(page_name) like '%sign%' then 'Login/Logout'
        when lower(page_name) like '%cust%' then 'Customer/User-Account'
        when lower(page_name) like '%account%' then 'Customer/User-Account'
        when lower(page_name) like '%user%' then 'Customer/User-Account'
        when lower(page_name) like '%card%' then 'Customer/User-Account'
        when lower(page_name) like '%track%' then 'Tracking'
        when lower(page_name) like '%search%' then 'Tracking'
        when lower(page_name) like '%trkid%' then 'Tracking'
        when lower(page_name) like '%notfound%' then 'Tracking'
        when lower(page_name) like '%sms%' then 'Tracking'
        when lower(page_name) like '%ship%' then 'Shipment/Delivery'
        when lower(page_name) like '%addres%' then 'Shipment/Delivery'
        when lower(page_name) like '%deliv%' then 'Shipment/Delivery'
        when lower(page_name) like '%bill%' then 'Billing'
        when lower(page_name) like '%invoice%' then 'Billing'
        when lower(page_name) like '%print%' then 'Print'
        when lower(page_name) like '%checkout%' then 'Checkout'
        when lower(page_name) like '%cart%' then 'Checkout'
        when lower(page_name) like '%rate%' then 'Rate'
        when lower(page_name) like '%contact%' then 'Contact-Us'
        when lower(page_name) like '%error%' then 'Error'
        when lower(page_name) like '%fedex/apps%' then 'FedEx App in Web'
        else 'Others'
      end

{% endmacro %}