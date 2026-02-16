SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.company_name,
    so.sales_order_id,
    so.order_date,
    so.sub_total
FROM
    {{ source('src_adw', 'customer') }} AS C
JOIN {{ source('src_adw', 'sales_order_header') }} AS so
ON c.customer_id = so.customer_id