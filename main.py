import requests
import pandas as pd
from datetime import datetime, timedelta
import json









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
        
        # Try the SQL query endpoint to get earnings_calendar table
        query_url = f"{url}?q=SELECT * FROM earnings_calendar ORDER BY date DESC LIMIT 1000"
        
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
            # Return upcoming earnings if no data for target week
            future_earnings = df[df[date_column] >= today.strftime('%Y-%m-%d')].head(10)
            return future_earnings, {}, {"total_count": 0, "week_start": week_start, "week_end": week_end}
        
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

if __name__ == "__main__":
    # Example usage - get next week's earnings
    earnings_df, earnings_by_day, summary_stats = get_earnings_data(weeks_ahead=1)
    
    if earnings_df is not None:
        formatted_summary = format_earnings_summary(earnings_df, earnings_by_day, summary_stats)
        
        # Example: Access the data programmatically
        total_companies = formatted_summary['total_companies']
        week_range = formatted_summary['week_range']
        all_symbols = formatted_summary['all_symbols']
        
        # Example: Get earnings for a specific day
        for date_str, day_info in formatted_summary['daily_breakdown'].items():
            day_name = day_info['day_name']
            symbols = day_info['symbols']
            count = day_info['count']
            # Do something with the data...
    else:
        # Handle error case
        error_msg = summary_stats.get('error', 'Unknown error')
