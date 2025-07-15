# InTheMemory

This project is my response to the Data Engineer technital test from InTheMemory

To run it, you'll need to set up an execution environment using [Poetry](https://python-poetry.org/). All requirements are specified in the ``pyproject.toml`` and can be installed into your Poetry env by running

```bash
poetry install --no-root
```

You'll also need to add the ``connection_string.txt`` at the root of this project to access data.

Once all the requirements are met, you can run the script by executing:

```bash
python app.py [--dates 2023-11-23 2023-11-24]
```

As you can see, I added an optionnal "dates" parameter. This parameter allows the user to run the ingestion script for specific dates if needed. It can be useful to reingest existing dates, dates that contained incorrect data or dates that weren't processed properly. If you don't provide ``--dates`` to app.py, the script will automatically take the current execution date as parameter, meaning it will ingest files corresponding to the day the program is run.


---

## Referential Files

The script starts by processing what I call **referential files**: `clients.csv`, `stores.csv`, and `products.csv`. These files are not sent daily and contain metadata related to transactions.

After checking the schema and column types (if any check fails, the file is moved to an `error` folder for manual review), the script adds two new fields, built from `latlng` column of stores.csv, ``latitude`` and ``longitude``.

---

## Transaction Files

After the referential files are loaded into DataFrames, the script processes the **transaction files**.

It:
- Lists the blobs at the root of the Azure container (where the files were located when I listed it).
- Keeps only the files whose names begin with `transactions`.
- Filters the files according to the `--dates` parameter.
- Downloads the matching files.
- Performs the same validation checks as for the referential files.
- Concatenates the validated files into a single transactions DataFrame.

Two main transformations follows: 
1. Creation of a datetime column containing the aggregation of date, hour and minute columns, parsed as datetime 
2. Addition of the account_id field, retrieved from clients dataframe (that's why the script ingested referential tables before transactions).

---

## Output

Once it's done, the script groups transactions by date and writes the resulting dataframes to Azure Blob Storage in Parquet format. I chose to only use date as a partition key, after looking at the size of the transactions files. One transaction parquet for a day is approximately 8 Mo, which is fine. It's also better to havea single 8 Mo file than hundreds of tiny ones (better performance when reading those files). 

The output location is :
```
formatted/transactions_{date}/transactions.parquet
```
 I chose to write results as parquet files because it's a convenient format to be used after an ingestion process like this. The script ingested raw data, formatted some fields, casting some to the right format, now we can use those parquet files to do heavier transformations or unions using spark for example. 

I also added unit tests to all the utils functions i coded in utils.py. Unfortunately, one of those test (``test_retrieve_csv_data``) is giving me an error (issue with the mock of the reading of blob not giving me bytes when it should...) and i couldn't find the resolution, but the other ones work just fine ! 

# What I wanted to add:

1. **Azure Function Deployment**

    I believe this script should be used in a serverless Azure Function. The transformations are lightweight, files are not so heavy neither even if we want to reingest multiple days. It would also enables to trigger the script automatically on presence of file in azure blob storage. Since there's no fixed number of files and there are several files per day, i would suggest to ask for the sending of an "end of transmission" file from the customer. This file would be empty and would be just a proof that all files have been sent for the day ! Alternatively, we could schedule this process, but that would perhaps lead to incomplete ingestion, due to delays (as noted). Even if we schedule it at 9:30 AM, we can't guarantee that all files have arrived.
    
2. **CI/CD Pipeline**  
    As I would have built an Azure Function, I also wanted to create a CI/CD pipeline, that would:
    - Runs unit tests on each commit.
    - Checks test coverage.
    - Packages and deploys the code to different environments. 
    
     This would help track which version of the code is deployed in each environment and quickly patch any remaining bugs.
3. **Enrichment with Store Coordinates**  
    I considered adding fields to the transactions table, such as store coordinates as I thought it would be a nice addition for spatial analysis : identifying where most transactions occur and visualizing the top-performing stores on a map. But I thought it would just make this table heavier and thought it would be best to add those fields in a more robust process (maybe from formatted tables to aggregated ones, the ones that are refined, transformed accordingly for dashboards). 