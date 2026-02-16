SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.company_name,
    sod.product_id,
    P.name,
    sod.order_qty,
    sod.unit_price,
    so.sales_order_id,
    so.order_date,
    so.sub_total,
    so.due_date,
    ROW_NUMBER() OVER(PARTITION BY so.sales_order_id ORDER BY so.sales_order_id) AS ORDER
FROM
    {{ source('src_adw', 'customer') }} AS c
JOIN {{ source('src_adw', 'sales_order_header') }} AS so
ON c.customer_id = so.customer_id
JOIN {{ source('src_adw', 'sales_order_detail') }} AS sod
ON so.sales_order_id = sod.sales_order_id
JOIN {{ source('src_adw', 'product') }} AS p
ON sod.product_id = P.product_id
ORDER BY so.sales_order_id;