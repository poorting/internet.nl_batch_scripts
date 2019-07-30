# Internet.nl helper scripts

Internet.nl is an initiative of the Dutch Internet Standards Platform that
helps you to check whether your website, email and internet connection use
modern and reliable Internet Standards. And if they don’t, what can you do
about it?

See the [internet.nl](https://internet.nl) website and/or its [source code on github](https://github.com/NLnetLabs/Internet.nl).

## Quick introduction

With the internet.nl website you can test individual domains, but if you need to test more domains (more than a few anyway) then it's probably easier to use the batch API of internet.nl instead. You can request access to the batch API by sending a mail to internet.nl at question@internet.nl or vraag@internet.nl

Note that newer versions of the website will allow you to upload a list as well and have it regularly tested via its new [dashboard](https://dashboard.internet.nl) functionality. The dashboard is currently still in beta.

The two scripts in this repository allow you to submit domains from a .csv file to the internet.nl API and convert the resulting JSON results back to a csv file for further processing.

The submit-domains script does the submitting bit. After successful submission a simple shell script will be created that will download results of the batch test and display it on screen. When the test is not finished yet it will simply show the response of the API instead.

The result-to-csv script expects a JSON file and extracts the useful bits into a .csv file, which you can then import into Excel (or other tools/programs/things that can ingest CSVs). The script extracts a number of categories and views from the JSON file. Which categories and views you get depends on the settings internet.nl configures for your account. See the [Batch API documentation](https://github.com/NLnetLabs/Internet.nl/blob/master/documentation/batch_http_api.md) for more information on categories and views.
You can change the categories and views the script looks for by changing the categories_keys and views_keys dictionaries in the script itself.

## Getting started

### Get credentials
The first and foremost step is to nicely ask vraag@internet.nl to be granted access to the Batch API.

Once you have received the account details, create a file called 'credentials' in the same directory as the scripts and add the account information to it like so:

```
machine batch.internet.nl login <your_account_name> password <your_account_password>
```
The scripts will use this information to authenticate to the API when submitting a request or retrieving results.

### Setting up the basics
Then you have to prepare the .csv file to contain the domains you want to batch test. I find it easiest to modify the .xlsx file with Excel and then save that as a .csv file; but you can directly edit the .csv file as well of course.

Each line can contain 3 fields: the domain to be used for a web test, the domain to be used for a mail test, and a 'sector/type' field you can use to distinguish between various domains. This can be useful to group domains, for example per department responsible or the type of organisation.

### Submitting to the API

To submit your domains to the API for testing use the following command:
```
./submit-domains.py  <mail|web> [name = 'Test'] [domains CSV file = 'domains/domains.csv']
```

Since the API can test mail or web domains, you have to specify which of the two you want. You can optionally follow that with the name you want the test to have and the location where the .csv file to use is. If the defaults are fine by you then simply invoking the script followed by mail or web will do nicely.

Depending on the type of measurement (web or mail) the domain information in the corresponding column of the .csv file will be used.

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
But please do not keep running the script every few minutes, since that will unnecessarily put a load the machine(s); just try it every few hours at most.

Once the results are ready, the output of the script will be screenfuls of JSON data.

### Processing the results
Put the results in a file by redirecting the output, e.g.:

```
./getResult-Test-web > output.json
```
The 'result-to-csv' script can be used to convert the JSON file to a .csv.

```
./result-to-csv.py  <JSON results file> [domains CSV file]
```
The first argument is the JSON file you just created by redirecting the output. Optionally you can specify the domains .csv file you used to submit the measurement. If you specify a domains file then the script will add the 'sector/type' information from that file to every domain it reads the results of. If you don't need some sort of organisation into groups or types for further processing then there is no need to specify the domains file, but if you would like that information present in the resulting .csv (for example because you want to order the result by sector/type), then obviously you do need to specify it.

The script will automatically detect if the results are for a web or a mail type measurement.

If all is well then the script will output the csv formatted results to the screen, with the first line mentioning the fields present in the file.

You can redirect the results to a file if needed; e.g.

```
./result-to-csv.py  output.json domains/domains.csv > output.csv
```

The resulting file can then easily be imported into Excel or something similar for further processing and generation of figures.

## License

This project is licensed under the Apache License, Version 2.0 - see the
[LICENSE-Apache-2.0.txt](LICENSE-Apache-2.0.txt) file for details.
