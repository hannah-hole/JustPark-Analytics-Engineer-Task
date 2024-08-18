-- base_pricing__prices.sql

SELECT
    *
FROM
    {{ source('pricing', 'prices') }}
