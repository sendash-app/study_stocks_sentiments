# study_stocks_sentiments
A short study on the return of stock prices vs news/ tweets sentiments

## Vader with [`stock_market_lexicon`](https://github.com/nunomroliveira/stock_market_lexicon)
For sentiment analysis we rely on the [vaderSentiment](https://github.com/cjhutto/vaderSentiment) package. To install (here we assume you are using Python 3 and [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/); if you don't, you should):  
```
$ mkvirtualenv --python=/`which python3/` SENDASH
$ workon SENDASH
(SENDASH) $ pip install vaderSentiment
```

Then let's edit the `vaderSentiment` package by editing it's `vaderSentiment.py` file to look at our [`stock_lexicon.txt`](resources/stock_lexicon.txt) install of its `vaderSentiment.txt`. To find `vaderSentiment.py` you need to go to the `site-packages` directory (for this example, we are using python 3.6):
```
(SENDASH) $ cdvirtualenv
(SENDASH) $ cd lib/python3.6/site-packages/vaderSentiment
```  
The line you want to change is `line 209`.
