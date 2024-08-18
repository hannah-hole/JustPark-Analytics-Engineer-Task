-- intm_pricing__tariffs

with tariff_data as (
SELECT
    rule_id as tariff_id,
    -- here we extract the values of certain keys in each
    -- JSON tariff record (d):
    tariff->>'$.price' AS price,
    -- 'size' refers to the duration of the parking session:
    tariff->>'$.size' AS duration,
    tariff->>'$.applies_from' AS applies_from,
    tariff->>'$.applies_to' AS applies_to,
    tariff->>'$.interval' AS interval,
    tariff->>'$.repeat' AS repeat,
    (tariff->>'$.proportional')::BOOLEAN AS proportional,
    (tariff->>'$.monthly')::BOOLEAN AS monthly
    {# N.B. not exhaustive, many other fields in 'tariff' #}
FROM
    {{ ref('base_pricing__rules') }}
)
    SELECT
        ROW_NUMBER() OVER () AS pricing_tariffs_id,
        tariff_id,
        price,
        duration,
        applies_from,
        applies_to,
        interval,
        repeat, 
        proportional,
        monthly
    from 
        tariff_data