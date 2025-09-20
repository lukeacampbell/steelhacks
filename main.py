import yfinance as yf

# Initialize the search object with a query
search_results = yf.Search("Tesla")

# Get a list of stock quotes
quotes = search_results.quotes
print("Top 1 Quotes:")
for quote in quotes[:1]:
    print(quote)

# Get a list of news articles
news = search_results.news
print("\nTop 3 News Headlines:")
for article in news[:3]:
    print(article['title'])