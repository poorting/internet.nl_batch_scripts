# Internet.nl helper scripts

Internet.nl is an initiative of the Dutch Internet Standards Platform that
helps you to check whether your website and email use
modern and reliable Internet Standards. And if they don’t, gives suggestions on what needs to be done to improve it.

See the [internet.nl](https://internet.nl) website and/or its [source code on github](https://github.com/NLnetLabs/Internet.nl).

## Quick introduction

With the internet.nl website you can test individual domains, but if you need to test more domains (more than a few anyway) then it's probably easier to use the batch API of internet.nl instead. You can request access to the batch API by sending a mail to internet.nl at question@internet.nl or vraag@internet.nl

Note that newer versions of the website will allow you to upload a list as well and have it regularly tested via its new [dashboard](https://dashboard.internet.nl) functionality. The dashboard is currently still in beta.

The two scripts in this repository allow you to submit domains from an Excel (.xlsx) file to the internet.nl API and convert the resulting JSON results back to an Excel (.xlsx) file for further processing.

The submit-domains script does the submitting bit. After successful submission a simple shell script will be created that will download results of the batch test and display it on screen. While the test is not finished yet it will simply show the response of the API instead.

The result-to-excel script expects a JSON file and extracts all the information into an .xlsx file, which you can then open using Excel (or other tools/programs/things that can read xlsx files). The script extracts all the categories and views from the JSON file. Which categories and views you get depends on the settings internet.nl configures for your account. See the [Batch API documentation](https://github.com/NLnetLabs/Internet.nl/blob/master/documentation/batch_http_api.md) for more information on categories and views.
The script also adds the link at the end of each row that points to the results on internet.nl for the domain on that row. Optionally it can add information from the domains Excel file used for submitting domains, useful if it contains metadata you may need for further processing.

## Getting started

### Get credentials
The first and foremost step is to nicely ask vraag@internet.nl or question@internet.nl to be granted access to the Batch API.

Once you have received the account details, create a file called 'credentials' in the same directory as the scripts and add the account information to it like so:

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
./submit-domains.py  <mail|web> [name = 'Test'] [domains file = 'domains/domains.xlsx']
```

Since the API can test mail or web domains, you have to specify which of the two you want. You can optionally follow that with the name you want the test to have and the location where the .xlsx file to use is. If the defaults are fine by you then simply invoking the script followed by mail or web will do nicely.

Depending on the type of measurement (web or mail) the domain information in the corresponding column of the .xlsx file will be used.

The script will use the information from the credentials file to authenticate against the API and submit the list of domains. If successful then a shell script will be created called 'getResult-<name>-<type>', so with the defaults this script will either be called 'getResult-Test-web' or 'getResult-Test-mail' depending on the type of measurement you specified.

### Retrieving the results
Using the 'getResults-*' shell script you can retrieve the results of the batch measurement.
The shell script is nothing more than a curl command that also uses the 'credentials' file for account information and points to the right URL where the results will eventually be available.

Measurements will take some time to complete. If the measurement is still running the API/script will output something such as:

```
{"success": false, "message": "Batch request is running", "data": {"results": "https://batch.internet.nl/api/batch/v1.1/results/<long_number>/"}
```
After a while the message changes to something like this:

```
{"success": false, "message": "Report is being generated", "data": {"results": "https://batch.internet.nl/api/batch/v1.1/results/<long_number>/"}
```
But please do not keep running the script every few minutes, since that will unnecessarily put a load on the machine(s); just try it every hour or so.

Once the results are ready, the output of the script will be screenfuls of JSON data.

### Processing the results
Put the results in a file by redirecting the output, e.g.:

```
./getResult-Test-web > output.json
```
The 'result-to-xlsx' script can be used to convert the JSON file to an .xlsx file containing all the measurement results.

```
./result-to-xlsx.py  <JSON results file|JSON results directory> [domains xlsx file] [metadata_column_name[,md_col_name2, ...]]
```
The first argument is the JSON file you just created by redirecting the output. You can also refer to a directory in which case the script will convert all the .json files it finds there. Optionally you can specify the domains .xlsx file you used to submit the measurement. If you specify a domains file then the script will add the 'type' information from that file (from the column of the same name) to every domain it reads the results of. If you don't need some sort of organisation into groups or types for further processing then there is no need to specify the domains file, but if you would like that information present in the resulting .xlsx (for example because you want to order the result by type), then obviously you do need to specify it. You can specify multiple columns from the domains xlsx file to be combined in this way by separating the column names with a comma. As an example: to combine the Name and type with the measurement results the command would be something like this:

```
./results-to-xslx.py test.json domains/domains.xlsx type,Name
```

The script will automatically detect if the results are for a web or a mail type measurement.

If all is well then the script will output the xlsx formatted results to a file with the same name(s) as the .json file but then with an .xlsx extension.


## License

This project is licensed under the Apache License, Version 2.0 - see the
[LICENSE-Apache-2.0.txt](LICENSE-Apache-2.0.txt) file for details.
