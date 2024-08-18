-- dates

select 
    ROW_NUMBER() OVER () AS date_tariffs_id,
    * 
from {{ ref('intm_date__tariffs') }}