# ONS-pipeline

This is a data pipeline that scrapes item level CPI data from the Office of National Statistic's website (this [section](https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes) in particular), validates it, and loads it into a DuckDB database.

## Usage

Currently only an `environment.yml` is available for conda users. A `requirements.txt` will be made soon for pip + virtualenv.

To run the pipeline:
```
python -m run_pipeline
```

A `data/` and `database/` folder will be automatically created for the scraped data and final database respectively.
