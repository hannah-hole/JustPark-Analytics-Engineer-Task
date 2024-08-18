-- intm_date__tariffs

with tariff_data as (
SELECT
    rule_id as tariff_id,
    -- here we extract the values of certain keys in each
    -- JSON tariff record (d):
    -- tariff->>'$.applies_on_days[*]' AS applies_on_days,
    UNNEST(tariff->>'$.applies_on_days[*]')::INT AS day_of_week,
    tariff->>'$.applies_between[0]' AS applies_between,
FROM
    {{ ref('base_pricing__rules') }}
)
    SELECT DISTINCT
        tariff_id,
        day_of_week,
        JSON_EXTRACT_STRING(applies_between, '$[0]')   AS valid_from,
        JSON_EXTRACT_STRING(applies_between, '$[1]')   AS valid_to
    from 
        tariff_data