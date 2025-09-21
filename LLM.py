import os
import json
import time
from groq import Groq

# Initialize Groq client with environment variable
client = Groq(
    api_key=os.environ.get("GROQ_API_KEY", "")
)

def analyze_sentiment_for_company(company_data, ticker):
    """
    Analyze sentiment for a single company using Groq API
    """
    # Get article details
    articles = company_data.get('article_details', [])
    
    if not articles:
        return {"ticker": ticker, "sentiment_score": 0}
    
    # Prepare the data for analysis
    headlines = []
    for article in articles:
        headline = article.get('headline', '')
        if headline and headline != 'No headline':
            headlines.append(headline)
    
    if not headlines:
        return {"ticker": ticker, "sentiment_score": 0}
    
    # Create the prompt with all headlines for this company
    headlines_text = "\n".join([f"- {headline}" for headline in headlines])
    
    user_content = f"""
Company: {ticker}
Earnings Date: {company_data.get('earnings_date', 'Unknown')}

Article Headlines:
{headlines_text}

Please analyze the sentiment of these headlines for {ticker} and return a sentiment score from -10 (very negative) to +10 (very positive).
"""

    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": """You are a financial news sentiment analyst. I will provide JSON data that contains companies, their earnings date, and a list of article headlines with metadata.

Your task:

For each company in the JSON, analyze the tone and connotation of all article headlines in article_details.

Assign a sentiment score from -10 (very negative) to +10 (very positive), with 0 being neutral.

Output only in this format (one JSON array, no extra commentary):

[
{ "ticker": "EBF", "sentiment_score": +6 },
{ "ticker": "FEAM", "sentiment_score": -4 },
{ "ticker": "FLY", "sentiment_score": 0 }
]

Make sure every company in the input JSON has an entry, even if there are no articles (in that case, use 0)."""
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            model="llama-3.3-70b-versatile",
        )
        
        response_text = chat_completion.choices[0].message.content.strip()
        
        # Try to extract just the sentiment score from the response
        # Look for a number between -10 and 10
        import re
        score_match = re.search(r'[+-]?\d+', response_text)
        if score_match:
            score = int(score_match.group())
            # Clamp between -10 and 10
            score = max(-10, min(10, score))
        else:
            score = 0
            
        return {"ticker": ticker, "sentiment_score": score}
        
    except Exception as e:
        print(f"Error analyzing {ticker}: {str(e)}")
        return {"ticker": ticker, "sentiment_score": 0}

def process_earnings_sentiment(json_filename="earnings_news_urls.json"):
    """
    Process all companies from the earnings JSON file and calculate sentiment scores
    """
    print(f"Loading earnings data from {json_filename}...")
    
    # Load the earnings data
    with open(json_filename, 'r', encoding='utf-8') as f:
        earnings_data = json.load(f)
    
    companies = earnings_data.get('companies', {})
    print(f"Found {len(companies)} companies to analyze")
    
    sentiment_results = []
    
    for i, (ticker, company_data) in enumerate(companies.items(), 1):
        print(f"Analyzing {ticker}... ({i}/{len(companies)})")
        
        # Analyze sentiment for this company
        result = analyze_sentiment_for_company(company_data, ticker)
        sentiment_results.append(result)
        
        # Add small delay to be respectful to API
        time.sleep(1)
    
    # Save results to file
    output_filename = "earnings_sentiment_analysis.json"
    output_data = {
        "analysis_date": earnings_data.get('generated_at'),
        "earnings_week": earnings_data.get('earnings_week'),
        "total_companies_analyzed": len(sentiment_results),
        "sentiment_results": sentiment_results
    }
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Sentiment analysis complete!")
    print(f"📊 Analyzed {len(sentiment_results)} companies")
    print(f"💾 Results saved to '{output_filename}'")
    
    # Print summary
    print("\n" + "="*60)
    print("SENTIMENT ANALYSIS RESULTS")
    print("="*60)
    
    # Sort by sentiment score
    sorted_results = sorted(sentiment_results, key=lambda x: x['sentiment_score'], reverse=True)
    
    print(f"\n🟢 MOST POSITIVE SENTIMENT:")
    for result in sorted_results[:10]:
        if result['sentiment_score'] > 0:
            print(f"  {result['ticker']}: {result['sentiment_score']:+d}")
    
    print(f"\n🔴 MOST NEGATIVE SENTIMENT:")
    negative_results = [r for r in sorted_results if r['sentiment_score'] < 0]
    for result in negative_results[-10:]:
        print(f"  {result['ticker']}: {result['sentiment_score']:+d}")
    
    print(f"\n📊 NEUTRAL (0 score): {len([r for r in sentiment_results if r['sentiment_score'] == 0])} companies")
    
    return {result['ticker']: result for result in sentiment_results}

if __name__ == "__main__":
    """
    Test the Groq API with a simple example first
    """
    print("Testing Groq API connection...")
    
    try:
        # Test API connection
        test_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant."
                },
                {
                    "role": "user",
                    "content": "Explain the importance of fast language models in one sentence.",
                }
            ],
            model="llama-3.3-70b-versatile",
        )
        
        print("✅ Groq API connection successful!")
        print(f"Response: {test_completion.choices[0].message.content}")
        print()
        
    except Exception as e:
        print(f"❌ Error connecting to Groq API: {e}")
        print("Please check your API key and internet connection.")
        exit(1)
    
    # Process earnings sentiment analysis
    try:
        print("Starting earnings sentiment analysis...")
        results = process_earnings_sentiment()
        print("\n🎉 Analysis completed successfully!")
        
    except FileNotFoundError:
        print("❌ earnings_news_urls.json not found. Please run main.py first to generate the data.")
    except Exception as e:
        print(f"❌ Error during sentiment analysis: {e}")
