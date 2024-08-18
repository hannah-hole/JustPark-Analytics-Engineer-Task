# Sample code to access the results of the dbt project and answer the two objectives

import duckdb
import pandas as pd
from pandasql import sqldf

conn = duckdb.connect("./pricing.duckdb")
prices = conn.execute("select * from main_mart.prices;").fetchdf()
tariffs = conn.execute("select * from main_mart.tariffs;").fetchdf()
dates = conn.execute("select * from main_mart.dates;").fetchdf()
conn.close()


#Sample query to allow us to easily look up the _current_ price that would be charged on different days of the week for parking sessions of various durations, at a specific location
query1="""
SELECT 
    p.location_id,
    t.duration,
    d.day_of_week,
    t.applies_from,
    t.applies_to,
    t.price
FROM prices AS p
LEFT JOIN tariffs t ON p.tariff_id = t.tariff_id
LEFT JOIN dates d ON d.tariff_id = t.tariff_id
WHERE p.location_id = 503495 
    AND t.interval='hours'
    AND t.monthly IS NULL
    AND p.deleted_at IS NULL;
 """
print(sqldf(query1))

#Sample query to easily look up how the price for a one hour parking session at a specific location has changed over time
query2="""
SELECT 
    p.location_id,
    substr(p.created_at,1, 10) as date,
    t.price
FROM prices AS p
LEFT JOIN tariffs t ON p.tariff_id = t.tariff_id
WHERE p.location_id = 185929
    AND t.duration = 1
    AND t.interval='hours'
    AND t.monthly IS NULL
ORDER BY p.created_at;
"""
print(sqldf(query2))

