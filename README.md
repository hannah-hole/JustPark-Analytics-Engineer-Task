# analytics_engineer_exercise

This is a technical task for mid-level analytics engineers.

The aim is to build some DBT models that fulfil certain objectives.

First let us define some terms:

- a parking location is a parking spot or car park where JustPark offers parking
- a parking session is when a parking spot is used for a particular duration, starting at some specific time/date
- a price is a set of rules you can use to calculate the cost of a parking session
- a tariff is member of the set of rules (e.g. 'if parking on a Saturday for 2 hours starting after 4pm, the hourly cost is £4.00')

**The objectives of the exercise are to build DBT models (e.g. in a star schema) that:**

1. allow us to easily look up the _current_ price that would be charged on different days of the week for parking sessions of various durations, at a specific location
2. allow us to easily look up how the price for a one hour parking session at a specific location has changed over time

It's important that these models be well-documented.

## Getting started

We are using two key technologies here:

- DuckDB (a hyper-flexible column-store database) can be used as a kind of 'local data warehouse'
- DBT is a popular data transformation tool for data warehousing purposes

For this project we're managing everything via `poetry`. You can install it [using these instructions](https://python-poetry.org/docs/#installing-with-the-official-installer). You also need to have Python installed on your system. If you run `poetry install` you will end up with `duckdb` and `dbt` installed in a local virtual environment.

> This isn't strictly necessary if you already have the Python modules `dbt-core`, `dbt-duckdb`, and `duckdb` installed on your system. It's arranged for convenience.

You can then run DBT commands prefixed with `poetry run` (to run them inside the virtual environment), e.g. `poetry run dbt run`. **We recommend trying this first**, to ensure everything works as it should.

## What to do

There is an example dataset in the warehouse file `pricing.duckdb`. It contains

- two source tables `main.prices` and `main.rules`
- two example, incomplete DBT models `main_base.base_pricing__rules` and `main_intm.intm_pricing__tariffs`

The two source tables are analogous to real tables in the JustPark database, albeit simplified and much smaller. Additional details may be found in `models/_sources/_sources.yml`, but in brief:

- `prices` represents the history of which pricing logic applied at particular times in the past
- `rules` contains the actual pricing rules, in a nested JSON structure

The example DBT models are defined in `models/base/base_pricing__rules.sql` and `models/intm/intm_pricing__tariffs.sql`. These are there mostly to demonstrate some DuckDB syntax for working with JSON data (since candidates are not expected to be familiar with this - [here is a reference](https://duckdb.org/docs/extensions/json.html#json-extraction-functions) in case needed).

(there is also `pricing_original.duckdb`, this is the original warehouse file _before_ the example DBT models have been run; it's there in case you get into difficulty and need a fresh start)

**Take some time to explore the project and the source tables before beginning.** There are many ways to read `duckdb` files. One way is to use Python to read the table into a DataFrame:

```
import duckdb

conn = duckdb.connect("./pricing.duckdb")
prices = conn.execute("SELECT * FROM prices;).fetchdf()
conn.close()
```

... however there are many other options.

How you choose to structure your solution is completely up to you. What we're looking for is

- a working project (i.e. `dbt run` will successfully build the models)
- a well-documented project (i.e. a colleague could maintain and extend the work without difficulty)
- a project that fulfils the two objectives stated at the beginning

We estimate that this will take 1-2 hours, to be done at the candidate's convenience. It's okay if you take longer, just tell us how long you took when you submit your solution.

Feel free to ask clarifying questions about this exercise. You can email these to `padraig.alton@justpark.com`.
