-- intm_pricing__prices
SELECT
    price_id,
    location_id,
    pricing_mode,
    created_at,
    deleted_at,
    rule_id as tariff_id
FROM
    {{ ref('base_pricing__prices') }}
