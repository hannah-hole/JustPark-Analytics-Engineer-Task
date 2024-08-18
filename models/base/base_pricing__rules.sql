-- base_pricing__rules.sql

SELECT
    rule_id,
    created_at,
    -- 'tariffs' is a JSON array, here we read it out and unwrap
    -- the contents into one record per element:
    UNNEST(tariffs->'$[*]') AS tariff,
FROM
    {{ source('pricing', 'rules') }}
