# Pretty Eikon

A package for data extraction and preprocessing of Eikon's company timeseries data and news

## Table of contents

* [Requirements](#requirements)
* [Time series](#time-series)
* [News](#news)

## Requirements
* pandas
* eikon
* eventlet
* json
* bs4


## Time series

Company time series can be imported with the TimeSeries class

```
import dataproviders as dp
timeseries = dp.TimeSeries(eikon_app_key = eikon_app_key,
                         data = data,
                         outputdir = outputdir,
                         dateto = dateto,
                         datefrom = datefrom)
```
where ```data``` is the list of company codes or rics to import the data for,
outputdir is the path to the outputdir where the data will be stored,
dateto and datefrom are either strings of the form YYYY-MM-DD or datetime() objects

You can import all company data with

```
timeseries.time_import()
```

## News

Company news can be imported with the NewsProvider class

```
import dataproviders as dp
news = dp.NewsProvider(eikon_app_key  = eikon_app_key,
                          data = data,
                          outputdir = outputdir,
                          dateto = dateto,
                          datefrom = datefrom)
```

where `data`is the list of company codes to import the data for,
outputdir is the path to the outputdir where the data will be stored,
dateto and datefrom are either strings of the form YYYY-MM-DD or datetime() objects

You can import news for all companies in the `data` by invoking

```
news.mine_news(do_clean)
```

where do_clean = True if the preprocessed text body should be outputted instead of the raw html from eikon.

Or, to import one day (date) of news for a single company (ric)

```
news.do_day(date, ric, do_clean)
```

You can use the preprocessing method to clean a raw html with

```
news.html_cleaner(self, pathtodir, destindir)
```

where pathtodir is the directory containing the raw html textfile(s) and destindir is the output directory where cleaned files are written with the same filename as in the original directory.
