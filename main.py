import requests
import pandas as pd
from datetime import datetime, timedelta
import finnhub

finnhub_client = finnhub.Client(api_key="")

def get_earnings_data_for_week(week_start_date):
    url = "https://www.dolthub.com/api/v1alpha1/post-no-preference/earnings/master"
    try:
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        query_url = f"{url}?q=SELECT * FROM earnings_calendar ORDER BY date DESC LIMIT 1000"
        response = requests.get(query_url, headers=headers)
        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}"}
        data = response.json()
        if 'rows' not in data:
            return {"error": "No data found in response"}
        df = pd.DataFrame(data['rows'])
        if df.empty:
            return {"error": "No earnings data available"}
        date_column = 'date' if 'date' in df.columns else df.columns[0]
        df[date_column] = pd.to_datetime(df[date_column])
        week_start = week_start_date
        week_end = week_start + timedelta(days=6)
        week_earnings = df[
            (df[date_column] >= week_start.strftime('%Y-%m-%d')) &
            (df[date_column] <= week_end.strftime('%Y-%m-%d'))
        ]
        if week_earnings.empty:
            return {"error": "No earnings data for this week"}
        result = {}
        for date, group in week_earnings.groupby(date_column):
            tickers = group['act_symbol'].tolist()
            day_name = date.strftime('%A')
            news_urls = {}
            for ticker in tickers:
                today = date
                month_ago = today - timedelta(days=30)
                try:
                    news = finnhub_client.company_news(ticker, _from=str(month_ago.date()), to=str(today.date()))
                    urls = [article['url'] for article in news if 'url' in article]
                except Exception:
                    urls = []
                news_urls[ticker] = urls
            result[date.strftime('%Y-%m-%d')] = {
                'day_name': day_name,
                'tickers': tickers,
                'total_companies': len(tickers),
                'news_urls': news_urls
            }
        return result
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    today = datetime.now()
    next_monday = today + timedelta(days=(7 - today.weekday()) % 7)
    week_data = get_earnings_data_for_week(next_monday)
    if 'error' in week_data:
        print('Error:', week_data['error'])
    else:
        for date_str, info in week_data.items():
            print(f"{info['day_name']} ({date_str}): {info['total_companies']} companies")
            for ticker in info['tickers']:
                print(f"  {ticker} news URLs:")
                for url in info['news_urls'].get(ticker, []):
                    print(f"    {url}")
            print()