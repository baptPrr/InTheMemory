# InTheMemory

This project is my answer to the Data Engineer tech test of InTheMemory

To execute it, you'll need to create an execution environment using poetry. All requirements are specified in the pyproject.toml and can be installed in your poetry env by running
`poetry install --no-root`

You'll also need to add the ``connection_string.txt`` at the root of this project to access data.

Once you have all the requirements, you can run my script by calling
`python app.py [--dates 2023-11-23 2023-11-24]`

As you can see, I added an optionnal "dates" parameter. This parameter enables the user to launch the ingestion script for specific dates if it is needed. That can be usefull to reingest existing dates, dates that contained wrong data or dates that didn't run as expected. If you don't give --dates to app.py, it will automatically take the current date (date of execution) as parameter. That means that it will ingest files from the date of the execution of the program.

My script will begin by transforming with I call referential files : clients, stores and products. Those files are not to be sent every day, and contains data that gives intel about transactions. After columns and schema check (if one of the check is not satisfied, the file is moved to an ``error`` folder, waiting to be examined by someone), the script adds two new fields, built from `latlng` column of stores.csv, contaning ``latitude`` and ``longitude``.

After having read those referential files and having them as dataframe, the script will read transactions files. It will list blobs at the root of the azure container (files seemed to be there when i listed the container) only if filename begins with ``transactions``, and will keep only those matching the requested ``dates`` parameter. Once it has this list of blob names, it will retrieve them, do the same checks that have been done for stores, products and clients, and if successfull, will be concatenate to an empty transactions dataframe.

2 transformations happened then, creation of a datetime column containing the aggreagtion of date, hour and minute column, parsed as datetime and the addition of the account_id field, retrieved from clients dataframe (that's why the script ingested referential tables before transactions).

Once it's done, the script will group transactions by date, to write those dataframes per date, in the azure blob storage, in parquet format. I chose to only use date as a partition key, after looking at the size of the transactions files. One transaction parquet for a day is approximately 8 Mo, which is fine. It's also better to have one file of 8 Mo than hundreds tiny ones (better performance when reading those files). The location is ``formatted/transactions_{date}/transactions.parquet``. I chose to write results as parquet files because it's a convenient format to be used after an ingestion process like this. The script ingested raw data, formatted some fields, casting some to the right format, now we can use those parquet files to do heavier transformations or unions using spark for example. 

I added unit tests to all the utils functions i coded in utils.py. Unfortunately, one of those test (``test_retrieve_csv_data``) is giving me an error (issue with the mock of the reading of blob not giving me bytes when it should...) and i couldn't find the resolution, but the other ones work just fine ! 

What I wanted to add:
1. I think this script should be used in a serverless azure function. The transformations are lightweight, files are not so heavy neither even if we want to reingest several days. It would also enables to launch the script automatically on presence of file in azure blob storage. As there's no fixed number of files and there are several files per day, i would suggest to ask for the sending of an "end of transmission" file from the customer. This file would be empty and would be just a proof that all files have been sent for the day ! We could also schedule this process, but that would perhaps lead to ingesting not all data, according to the note specifying that there's sometimes delay. We'll never be sure to have waited all the files, even if we schedule the process at 9:30 am.
2. As I would have built an azure function, i also wanted to create a CI/CD pipeline, that would run unit test on each commit, check code coverage of those tests and ask for packaging and installation of the code in different cloud environments. Therefore, we could track what code is in which env, and patch quickly any bug that may have subsisted.
3. I hesitated to add fields to transactions table, like the coordinates of the store as I thought it would be a nice addition. It would give intel on where there are the most transactions, enabling access to a map of all stores and a nice visualisation of the most performing ones. But I thought it would just make this table heavier and thought it would be best to add those fields in a more robust process (maybe from formatted tables to aggregated ones, the ones that are refined, transformed accordingly for dashboards). 