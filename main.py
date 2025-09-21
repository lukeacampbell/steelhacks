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
        
        # Calculate target week dates first
        today = datetime.now()
        
        if weeks_ahead == 0:  # This week
            week_start = today - timedelta(days=today.weekday())  # Monday
        else:  # Future weeks
            days_until_target_monday = (7 * weeks_ahead) - today.weekday()
            if today.weekday() == 0 and weeks_ahead == 1:  # If today is Monday, get next Monday
                days_until_target_monday = 7
            week_start = today + timedelta(days=days_until_target_monday)
        
        week_end = week_start + timedelta(days=6)  # Sunday
        
        # Query earnings calendar for the specific week
        query_url = f"{url}?q=SELECT * FROM earnings_calendar WHERE date >= '{week_start.strftime('%Y-%m-%d')}' AND date <= '{week_end.strftime('%Y-%m-%d')}' ORDER BY date ASC"
        
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
            # No earnings for the target week
            return None, {}, {"total_count": 0, "week_start": week_start, "week_end": week_end, "error": "No earnings for target week"}
        
        # Convert date column to datetime for grouping
        date_column = 'date' if 'date' in df.columns else df.columns[0]
        df[date_column] = pd.to_datetime(df[date_column])
        
        # Group earnings by day - ignore timing
        earnings_by_day = {}
        for date, group in df.groupby('date'):
            day_data = {
                'symbols': group['act_symbol'].tolist(),
                'count': len(group)
            }
            earnings_by_day[date.strftime('%Y-%m-%d')] = day_data
        
        # Summary statistics
        summary_stats = {
            'total_count': len(df),
            'week_start': week_start,
            'week_end': week_end,
            'days_with_earnings': len(earnings_by_day),
            'total_records_fetched': len(df)
        }
        
        return df, earnings_by_day, summary_stats
        
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

def save_urls_to_json(news_data, earnings_by_day, filename="earnings_news_urls.json"):
    """
    Save ticker symbols and URLs to JSON file for Gemini API
    """
    # Create structured data for Gemini API
    gemini_data = {
        "earnings_week": "2025-09-22 to 2025-09-28",
        "generated_at": datetime.now().isoformat(),
        "total_companies": len(news_data),
        "companies": {}
    }
    
    # Add earnings day information and URLs for each ticker
    for date_str, day_data in earnings_by_day.items():
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        day_name = date_obj.strftime('%A')
        
        for symbol in day_data['symbols']:
            symbol_news = news_data.get(symbol, {})
            urls = symbol_news.get('urls', [])
            
            gemini_data["companies"][symbol] = {
                "earnings_date": date_str,
                "earnings_day": day_name,
                "article_count": len(urls),
                "urls": [article['url'] for article in urls],  # Just the URLs for Gemini
                "article_details": urls  # Full article data if needed
            }
    
    # Save to JSON file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(gemini_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Saved {len(gemini_data['companies'])} companies and their URLs to '{filename}'")
    print(f"📊 Total URLs saved: {sum(len(company['urls']) for company in gemini_data['companies'].values())}")
    
    return filename

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
    # Get next week's earnings data
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
    print("Fetching news data...")
    
    # Get all symbols for news lookup
    all_symbols = formatted_summary['all_symbols']
    
    print(f"\n\nTotal symbols with earnings this week: {len(all_symbols)}")
    
    # Get news URLs for symbols
    news_data = get_company_news_urls(all_symbols, days_back=30)
    
    # Print daily breakdown with news counts
    print("\n" + "="*80)
    print("DAILY BREAKDOWN WITH NEWS ARTICLE COUNTS")
    print("="*80)
    
    # Convert date strings to day names for better display
    from datetime import datetime
    for date_str, day_data in earnings_by_day.items():
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        day_name = date_obj.strftime('%A')
        symbols = day_data['symbols']
        
        print(f"\n{day_name} ({date_str}):")
        for symbol in symbols:
            article_count = news_data.get(symbol, {}).get('article_count', 0)
            print(f"  {symbol}: {article_count} articles")
        
        total_articles = sum(news_data.get(symbol, {}).get('article_count', 0) for symbol in symbols)
        print(f"  Total articles for {day_name}: {total_articles}")
        print(f"  Companies reporting: {len(symbols)}")
    
    # Save URLs and ticker data to JSON file for Gemini API
    json_filename = save_urls_to_json(news_data, earnings_by_day)
    
    # Run sentiment analysis with Gemini
    print("\n" + "="*80)
    print("STARTING SENTIMENT ANALYSIS WITH GEMINI")
    print("="*80)
    
    try:
        from gemini import process_earnings_sentiment
        sentiment_results = process_earnings_sentiment(json_filename)
        
        # Print final summary
        print("\n" + "="*80)
        print("FINAL SENTIMENT SUMMARY")
        print("="*80)
        
        # Sort by sentiment score
        sorted_results = sorted(sentiment_results.items(), key=lambda x: x[1]['average_sentiment'], reverse=True)
        
        print(f"\n🟢 MOST POSITIVE SENTIMENT:")
        for symbol, data in sorted_results[:5]:
            print(f"  {symbol}: {data['average_sentiment']} ({data['article_count']} articles) - {data['earnings_day']}")
        
        print(f"\n🔴 MOST NEGATIVE SENTIMENT:")
        for symbol, data in sorted_results[-5:]:
            print(f"  {symbol}: {data['average_sentiment']} ({data['article_count']} articles) - {data['earnings_day']}")
            
    except ImportError:
        print("⚠️  Gemini module not available. Run 'python gemini.py' separately for sentiment analysis.")
    except Exception as e:
        print(f"⚠️  Error running sentiment analysis: {e}")