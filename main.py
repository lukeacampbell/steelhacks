import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import finnhub
import os
import time
from collections import defaultdict

# API key directly in code
API_KEY = "d37g7u1r01qskreg40ngd37g7u1r01qskreg40o0"
finnhub_client = finnhub.Client(api_key=API_KEY)

def get_earnings_data(weeks_ahead=1):
    """
    Fetch earnings calendar data from Dolthub and filter for specified week ahead
    
    Args:
        weeks_ahead (int): Number of weeks ahead to fetch (1 = next week, 0 = this week, etc.)
    
    Returns:
        tuple: (earnings_dataframe, earnings_by_day_dict, summary_stats)
    """
    
    # Dolthub API endpoint for earnings calendar
    url = "https://www.dolthub.com/api/v1alpha1/post-no-preference/earnings/master"
    
    try:
        # Make request to Dolthub API
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Try the SQL query endpoint to get earnings_calendar table - try getting more recent data
        query_url = f"{url}?q=SELECT * FROM earnings_calendar WHERE date >= '2025-09-20' ORDER BY date ASC LIMIT 1000"
        
        response = requests.get(query_url, headers=headers)
        
        if response.status_code != 200:
            return None, None, {"error": f"HTTP {response.status_code}"}
        
        # Parse the response
        data = response.json()
        
        if 'rows' not in data:
            return None, None, {"error": "No data found in response"}
        
        # Convert to DataFrame
        df = pd.DataFrame(data['rows'])
        
        if df.empty:
            return None, None, {"error": "No earnings data available"}
        
        # Get current date and calculate target week's date range
        today = datetime.now()
        
        if weeks_ahead == 0:  # This week
            week_start = today - timedelta(days=today.weekday())  # Monday
        else:  # Future weeks
            days_until_target_monday = (7 * weeks_ahead) - today.weekday()
            if today.weekday() == 0 and weeks_ahead == 1:  # If today is Monday, get next Monday
                days_until_target_monday = 7
            week_start = today + timedelta(days=days_until_target_monday)
        
        week_end = week_start + timedelta(days=6)  # Sunday
        
        # Convert date column to datetime
        date_column = 'date' if 'date' in df.columns else df.columns[0]
        df[date_column] = pd.to_datetime(df[date_column])
        
        # Filter for target week
        week_earnings = df[
            (df[date_column] >= week_start.strftime('%Y-%m-%d')) & 
            (df[date_column] <= week_end.strftime('%Y-%m-%d'))
        ]
        
        if week_earnings.empty:
            # No earnings for the target week - don't use fallback, just return empty
            print(f"DEBUG: No earnings found for target week {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")
            return None, {}, {"total_count": 0, "week_start": week_start, "week_end": week_end, "error": "No earnings for target week"}
        
        # Group earnings by day - ignore timing
        earnings_by_day = {}
        for date, group in week_earnings.groupby('date'):
            day_data = {
                'symbols': group['act_symbol'].tolist(),
                'count': len(group)
            }
            earnings_by_day[date.strftime('%Y-%m-%d')] = day_data
        
        # Summary statistics
        summary_stats = {
            'total_count': len(week_earnings),
            'week_start': week_start,
            'week_end': week_end,
            'days_with_earnings': len(earnings_by_day),
            'total_records_fetched': len(df)
        }
        
        return week_earnings, earnings_by_day, summary_stats
        
    except Exception as e:
        return None, None, {"error": str(e)}

def format_earnings_summary(earnings_df, earnings_by_day, summary_stats):
    """
    Format earnings data into readable summary strings
    
    Returns:
        dict: Formatted summary data
    """
    if earnings_df is None or earnings_df.empty:
        return {"error": "No earnings data to format"}
    
    formatted_summary = {
        'total_companies': summary_stats['total_count'],
        'week_range': f"{summary_stats['week_start'].strftime('%Y-%m-%d')} to {summary_stats['week_end'].strftime('%Y-%m-%d')}",
        'daily_breakdown': {},
        'all_symbols': earnings_df['act_symbol'].tolist()
    }
    
    for date_str, day_data in earnings_by_day.items():
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_summary['daily_breakdown'][date_str] = {
            'day_name': date_obj.strftime('%A'),
            'symbols': day_data['symbols'],
            'count': day_data['count']
        }
    
    return formatted_summary

def get_company_news_urls(symbols, days_back=30):
    """
    Get news URLs for each ticker symbol from the last month
    
    Args:
        symbols (list): List of ticker symbols
        days_back (int): Number of days to look back for news (default 30)
    
    Returns:
        dict: Dictionary with ticker symbols as keys and news data as values
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # Convert to required format (YYYY-MM-DD)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    news_data = {}
    
    for i, symbol in enumerate(symbols):
        print(f"Fetching news for {symbol}... ({i+1}/{len(symbols)})")
        
        # Add delay to avoid hitting API rate limits (free tier is 60 calls per minute)
        if i > 0 and i % 50 == 0:  # Every 50 calls, wait a bit longer
            print("Pausing to avoid rate limits...")
            time.sleep(65)  # Wait just over a minute
        else:
            time.sleep(1.1)  # Small delay between calls
        
        # Get company news for the symbol
        news = finnhub_client.company_news(symbol, _from=start_str, to=end_str)
        
        if news:
            urls = []
            sources = set()
            
            for article in news:
                if 'url' in article and article['url']:
                    urls.append({
                        'url': article['url'],
                        'headline': article.get('headline', 'No headline'),
                        'source': article.get('source', 'Unknown'),
                        'datetime': article.get('datetime', 0)
                    })
                    sources.add(article.get('source', 'Unknown'))
            
            news_data[symbol] = {
                'urls': urls,
                'article_count': len(urls),
                'unique_sources': len(sources),
                'sources': list(sources)
            }
        else:
            news_data[symbol] = {
                'urls': [],
                'article_count': 0,
                'unique_sources': 0,
                'sources': []
            }
    
    return news_data

def print_news_summary(news_data):
    """
    Print summary of news data for all tickers
    """
    print("\n" + "="*80)
    print("NEWS SUMMARY FOR ALL TICKERS")
    print("="*80)
    
    total_articles = 0
    total_sources = set()
    
    for symbol, data in news_data.items():
        article_count = data['article_count']
        unique_sources = data['unique_sources']
        sources = data['sources']
        
        total_articles += article_count
        total_sources.update(sources)
        
        print(f"\n{symbol}:")
        print(f"  Articles: {article_count}")
        print(f"  Unique news sources: {unique_sources}")
        
        if sources:
            print(f"  Sources: {', '.join(sources)}")
    
    print(f"\n" + "-"*50)
    print(f"TOTAL SUMMARY:")
    print(f"Total articles across all tickers: {total_articles}")
    print(f"Total unique news sources: {len(total_sources)}")
    print(f"All sources: {', '.join(sorted(total_sources))}")

def print_all_urls(news_data):
    """
    Print all URLs for each ticker
    """
    print("\n" + "="*80)
    print("ALL NEWS URLS BY TICKER")
    print("="*80)
    
    for symbol, data in news_data.items():
        urls = data['urls']
        
        print(f"\n{symbol} ({len(urls)} articles):")
        print("-" * 40)
        
        if urls:
            for i, article in enumerate(urls, 1):
                print(f"{i}. {article['headline']}")
                print(f"   Source: {article['source']}")
                print(f"   URL: {article['url']}")
                print()
        else:
            print("No articles found")

if __name__ == "__main__":
    # Example usage - get next week's earnings
    earnings_df, earnings_by_day, summary_stats = get_earnings_data(weeks_ahead=1)
    
    if earnings_df is not None:
        formatted_summary = format_earnings_summary(earnings_df, earnings_by_day, summary_stats)
    else:
        # Handle case where no earnings data is found
        if 'error' in summary_stats:
            print(f"No earnings data found: {summary_stats['error']}")
        else:
            print("No earnings data found for next week")
        print(f"Searched week: {summary_stats['week_start'].strftime('%Y-%m-%d')} to {summary_stats['week_end'].strftime('%Y-%m-%d')}")
        exit()
    
    print(f"Earnings for week: {formatted_summary['week_range']}")
    print(f"Total companies reporting: {formatted_summary['total_companies']}")
    print("-" * 50)
    
    # Print each day's symbols
    for date_str, day_info in formatted_summary['daily_breakdown'].items():
        day_name = day_info['day_name']
        symbols = day_info['symbols']
        count = day_info['count']
        
        print(f"\n{day_name} ({date_str}):")
        print(f"Companies reporting: {count}")
        if symbols:
            print(f"Symbols: {', '.join(symbols)}")
            # Print each symbol on its own line for better readability
            print("Earnings today:")
            for i, symbol in enumerate(symbols, 1):
                print(f"  {i}. {symbol}")
        else:
            print("No earnings scheduled")
    
    # Get all symbols for news lookup
    all_symbols = formatted_summary['all_symbols']
    
    print(f"\n\nTotal symbols with earnings this week: {len(all_symbols)}")
    
    # Comment out news fetching for now
    # # For testing, let's use first 20 symbols to avoid hitting rate limits too quickly
    # test_symbols = all_symbols[:20]
    # 
    # print(f"\n\nFetching news for {len(test_symbols)} companies (subset of {len(all_symbols)} total)...")
    # 
    # # Get news URLs for symbols
    # news_data = get_company_news_urls(test_symbols, days_back=30)
    # 
    # # Print summary
    # print_news_summary(news_data)
    # 
    # # Uncomment if you want to see all URLs
    # print_all_urls(news_data)
