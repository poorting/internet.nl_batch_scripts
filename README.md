# Internet.nl helper scripts

Internet.nl is an initiative of the Dutch Internet Standards Platform that helps you to check whether your website and email use
modern and reliable Internet Standards. And if they don’t, gives suggestions on what needs to be done to improve it.

See the [internet.nl](https://internet.nl) website and/or its [source code on github](https://github.com/NLnetLabs/Internet.nl).

## Quick introduction

With the internet.nl website you can test individual domains, but if you need to test more domains (more than a few anyway) then it's probably easier to use the batch API of internet.nl instead. You can request access to the batch API by sending a mail to internet.nl at question@internet.nl or vraag@internet.nl

Note that newer versions of the website will allow you to upload a list as well and have it regularly tested via its new [dashboard](https://dashboard.internet.nl) functionality, which suits most use cases. If you want to do some post-processing or other things not provided by the dashboard then these scripts may be useful.

The scripts in this repository allow you to submit domains from an Excel (.xlsx) file to the internet.nl API and convert the resulting JSON results. One script converts the JSON results back to an Excel (.xlsx) file for further processing, another outputs a text file that can be written/imported into an influx database. Not really useful for normal influx purposes, but you can use it for creating different queries to suit your needs.

The ``submit-domains`` script does the submitting bit. With the ``batch-request`` script you can query status and get the results.

The result-to-excel script expects a JSON file and extracts all the information into an .xlsx file of the same name (but different extension), which you can then open using Excel (or other tools/programs/things that can read xlsx files). The script extracts all the top level results and the individual test results from the JSON file. See the [Batch API documentation](https://github.com/NLnetLabs/Internet.nl/blob/master/documentation/batch_http_api.md) for more information on categories and views.
The script also adds the link at the end of each row that points to the results on internet.nl for the domain on that row. Optionally it can add information from the domains Excel file used for submitting domains, which is useful if that Excel file contains metadata you may need for further processing.

## Getting started

### Get credentials
The first and foremost step is to nicely ask vraag@internet.nl or question@internet.nl to be granted access to the Batch API.

Once you have received the account details add the account information to the *credentials* file like so:

```
machine batch.internet.nl login <your_account_name> password <your_account_password>
```
The scripts will use this information to authenticate to the API when submitting a request or retrieving results.

### Setting up the basics
Then you have to prepare the .xlsx file to contain the domains you want to batch test.

Each line can contain 3 fields: the domain to be used for a web test, the domain to be used for a mail test, and a 'sector/type' field you can use to distinguish between various domains. This can be useful to group domains, for example per department responsible or the type of organisation.

### Submitting to the API

To submit your domains to the API for testing use the following command:
```
./submit-domains.py  <mail|web> [name = 'Test'] [domains file = 'domains/domains.xlsx'] [sheet_name=0]
```

Since the API can test mail or web domains, you have to specify which of the two you want. You can optionally follow that with the name you want the test to have and the location where the .xlsx file to use is. If the defaults are fine by you then simply invoking the script followed by mail or web will do nicely. Depending on the type of measurement (web or mail) the domain information in the corresponding column of the .xlsx file will be used. You can specify the name of the sheet from the .xslx file to use as the final argument. If you specify nothing the first sheet will be used by default. This allows you to have other sheets in the same .xlsx file, for example for testing purposes or simply for having multiple batches of domains in the same file rather than multiple ones.

The script will use the information from the credentials file to authenticate against the API and submit the list of domains.

### Retrieving the results

Measurements will take some time to complete. The batch-request script can be used to check on the progress. With this script you can list the batch requests, get the status of an individual request, delete an outstanding request (e.g. if it is blocking further measurements) and finally retrieve the results of a specific batch measurement.

```
Usage:
./batch-request.py  <list|stat|get|del> [<limit|request_id>]

./batch-request.py  list [limit]         - lists the requests, up to [limit] results. default/0 is all
./batch-request.py  stat <request_id>    - gets the status of the request
./batch-request.py  get <request_id>     - gets the results of the request
./batch-request.py  del <request_id>     - cancels the request
```

Use the *list* command to get an overview of all the known batch request. The latest requests will be listed first. You can limit the number of requests returned by specifying a limit as an argument. Default is to list all.

Using the request_id for a specific batch (from the *list* command) you can use the *stat* command to get the status of that specific batch request. You can use either the *list* of the *stat* command to check on the status of requests (the format of the output is the same), but note that just using the *list* command will not trigger generating the report. That is: a batch measurement will stop at the status 'generating' if you only use the *list* command. Retrieving the status of that individual request using the *stat* command will trigger the generation of the report. 

### Processing the results
Once the status is 'done', the report can be retrieved by using the *get* command, e.g.:
```
./batch-request get cb7be7ed776e464796cd8514c6e2a0e7
```
This will retrieve the results of the measurement batch identified by ``cb7be7ed776e464796cd8514c6e2a0e7``.<br/>You can put the results in a file by redirecting the output, e.g.:

```
./batch-request <request_id> > output.json
```
The 'result-to-xlsx' script can be used to convert the JSON file to an .xlsx file containing all the measurement results.

```
./result-to-xlsx.py  <JSON results file|JSON results directory> [domains xlsx file] [metadata_column_name[,md_col_name2, ...]]
```
The first argument is the JSON file you just created by redirecting the output. You can also refer to a directory in which case the script will convert all the .json files it finds there. Optionally you can specify the domains .xlsx file you used to submit the original measurement. If you specify a domains file then the script will add the 'type' information from that file (from the column of the same name) to every domain it reads the results of. If you don't need some sort of organisation into groups or types for further processing then there is no need to specify the domains file, but if you would like that information present in the resulting .xlsx (for example because you want to order the result by type), then obviously you do. You can specify multiple columns from the domains xlsx file to be combined in this way by separating the column names with a comma. As an example: to combine information from a *Name* and *type* column with the measurement results the command would be:

```
./results-to-xslx.py test.json domains/domains.xlsx type,Name
```

Please note that the column names cannot contain spaces.

The script will automatically detect if the results are for a web or a mail type measurement.

If all is well then the script will output the xlsx formatted results to a file with the same name(s) as the .json file but with an .xlsx extension.


## License

This project is licensed under the Apache License, Version 2.0 - see the
[LICENSE-Apache-2.0.txt](LICENSE-Apache-2.0.txt) file for details.
