SELECT
    s.sale_date,
    s.product_name,
    s.quantity,
    s.price, -- تم التعديل من total_price إلى price
    w.weather_description,
    w.temperature_celsius,
    w.city AS weather_city -- نأخذ المدينة من جدول الطقس فقط
FROM
    {{ source('weather_sales_project', 'sales') }} s
LEFT JOIN
    {{ source('weather_sales_project', 'weather_data') }} w
ON
    s.sale_date::date = w.timestamp_utc::date