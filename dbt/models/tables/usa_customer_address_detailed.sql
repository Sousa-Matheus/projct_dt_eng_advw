WITH usa_customer_adress_detailed AS (
  SELECT 
      c.customer_id,
      c.first_name,
      c.last_name,
      c.company_name,
      a.address_id,
      a.address_line1,
      a.address_line2,
      a.city,
      a.state_province,
      a.country_region,
      ca.address_type
  FROM {{ source('src_adw', 'customer') }} AS c
  JOIN {{ source('src_adw', 'customer_address') }} AS ca
  ON ca.customer_id = c.customer_id
  JOIN {{ source('src_adw', 'address') }} AS a
  ON ca.address_id = a.address_id
)

SELECT *
FROM usa_customer_adress_detailed
WHERE country_region = 'United States'
ORDER BY customer_id;