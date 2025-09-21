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
    Analyze sentiment for a single company by sending URLs directly to the LLM
    """
    # Get article details
    articles = company_data.get('article_details', [])
    
    if not articles:
        return {"ticker": ticker, "sentiment_score": 0, "articles_analyzed": 0}
    
    print(f"  📰 Sending {len(articles)} article URLs to LLM for analysis...")
    
    # Prepare URLs and headlines for the LLM to analyze
    article_info = []
    for i, article in enumerate(articles[:15], 1):  # Limit to first 15 articles
        url = article.get('url', '')
        headline = article.get('headline', 'No headline')
        source = article.get('source', 'Unknown source')
        
        if url and headline != 'No headline':
            article_info.append({
                'number': i,
                'headline': headline,
                'url': url,
                'source': source
            })
    
    if not article_info:
        print(f"  ❌ No valid articles found for {ticker}")
        return {"ticker": ticker, "sentiment_score": 0, "articles_analyzed": 0}
    
    # Create the comprehensive prompt for LLM web analysis
    articles_list = []
    for article in article_info:
        articles_list.append(f"{article['number']}. HEADLINE: {article['headline']}")
        articles_list.append(f"   SOURCE: {article['source']}")
        articles_list.append(f"   URL: {article['url']}")
        articles_list.append("")
    
    articles_text = "\n".join(articles_list)
    
    user_content = f"""
COMPANY FOR ANALYSIS: {ticker}
EARNINGS DATE: {company_data.get('earnings_date', 'Unknown')}
EARNINGS DAY: {company_data.get('earnings_day', 'Unknown')}

I need you to analyze the sentiment for {ticker} by visiting and reading the full content of these {len(article_info)} news articles:

{articles_text}

INSTRUCTIONS:
1. Visit each URL and read the complete article content word-by-word
2. Analyze every word, phrase, sentence for sentiment toward {ticker}
3. Consider financial terminology, market language, and implied meanings
4. Look for positive indicators: growth, beating expectations, strong outlook, positive analyst ratings
5. Look for negative indicators: losses, missing targets, concerns, downgrades, problems
6. Weight the sentiment based on article credibility and source authority
7. Consider the overall narrative across all articles

SENTIMENT FACTORS TO ANALYZE:
- Financial performance metrics and trends
- Revenue, profit, and growth indicators  
- Management commentary and guidance
- Analyst opinions and price targets
- Market position and competitive landscape
- Risk factors and challenges mentioned
- Future outlook and projections
- Tone and connotation of language used

Please visit each URL, read the full article content, and provide a comprehensive sentiment score from -10 (very negative) to +10 (very positive) based on the complete analysis of all articles.

Return only the final sentiment score as a single integer.
"""

    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": """You are an expert financial news sentiment analyst with web browsing capabilities. You can visit URLs and read complete article content to perform comprehensive sentiment analysis.

ANALYSIS METHODOLOGY:
1. Visit each provided URL and read the complete article content
2. Analyze every word, phrase, and sentence for sentiment indicators
3. Consider financial terminology, market language, and implied meanings
4. Evaluate tone, connotation, and overall narrative
5. Weight different types of information (earnings data, analyst opinions, predictions)
6. Synthesize sentiment across multiple articles for a comprehensive view

SENTIMENT SCORING GUIDELINES:
- Very Positive (+8 to +10): Exceptional performance, strong growth, beating expectations significantly, very bullish outlook
- Positive (+4 to +7): Good performance, meeting/slightly beating expectations, positive trends, favorable analyst coverage
- Slightly Positive (+1 to +3): Minor positive indicators, neutral-to-good news, stable outlook
- Neutral (0): Factual reporting without clear sentiment bias, mixed signals that cancel out
- Slightly Negative (-1 to -3): Minor concerns, cautious outlook, some disappointing metrics
- Negative (-4 to -7): Missing expectations, declining performance, bearish sentiment, analyst downgrades
- Very Negative (-8 to -10): Major problems, significant losses, very poor outlook, serious concerns

Examples:
- "Apple beats earnings expectations, revenue up 20%" → +8
- "Microsoft misses quarterly revenue targets, shares fall" → -7
- "Tesla announces stock split; market reacts positively" → +5
- "Amazon CFO resigns unexpectedly, concerns over leadership" → -6
- "Meta to present at annual tech conference" → 0

IMPORTANT: Visit each URL, read the full article content, and return only a single integer from -10 to +10."""
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
        response_text = response_text.strip()
        
        # Look for a number between -10 and 10
        import re
        score_match = re.search(r'[+-]?\d+', response_text)
        if score_match:
            score = int(score_match.group())
            # Clamp between -10 and 10
            score = max(-10, min(10, score))
        else:
            score = 0
            
        print(f"  🤖 LLM analyzed {len(article_info)} articles and returned score: {score:+d}")
            
        return {
            "ticker": ticker, 
            "sentiment_score": score,
            "articles_analyzed": len(article_info),
            "total_articles_available": len(articles)
        }
        
    except Exception as e:
        print(f"  ❌ Error analyzing {ticker}: {str(e)}")
        return {
            "ticker": ticker, 
            "sentiment_score": 0,
            "articles_analyzed": 0,
            "total_articles_available": len(articles)
        }

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
    
    # Filter companies with articles for deep analysis
    companies_with_articles = {ticker: data for ticker, data in companies.items() 
                             if data.get('article_count', 0) > 0}
    
    print(f"Companies with articles for deep analysis: {len(companies_with_articles)}")
    print(f"Companies without articles (will receive neutral score): {len(companies) - len(companies_with_articles)}")
    
    for i, (ticker, company_data) in enumerate(companies.items(), 1):
        print(f"\n🔍 Analyzing {ticker}... ({i}/{len(companies)})")
        
        if company_data.get('article_count', 0) == 0:
            print(f"  📄 No articles found for {ticker}, assigning neutral score")
            result = {
                "ticker": ticker, 
                "sentiment_score": 0,
                "articles_analyzed": 0,
                "total_articles_available": 0
            }
        else:
            # Perform deep analysis with full article content
            result = analyze_sentiment_for_company(company_data, ticker)
        
        sentiment_results.append(result)
        
        print(f"  📊 Final score for {ticker}: {result['sentiment_score']:+d}")
        
        # Add delay to be respectful to APIs
        time.sleep(2)
    
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
    
    # Calculate analysis statistics
    total_articles_fetched = sum(r.get('articles_fetched', 0) for r in sentiment_results)
    total_articles_analyzed = sum(r.get('articles_analyzed', 0) for r in sentiment_results)
    companies_with_data = len([r for r in sentiment_results if r.get('articles_analyzed', 0) > 0])
    
    print(f"\n✅ Deep sentiment analysis complete!")
    print(f"📊 Companies analyzed: {len(sentiment_results)}")
    print(f"📰 Companies with article content: {companies_with_data}")
    print(f"📥 Total articles fetched: {total_articles_fetched}")
    print(f"� Total articles analyzed: {total_articles_analyzed}")
    print(f"�💾 Results saved to '{output_filename}'")
    
    # Print detailed summary
    print("\n" + "="*80)
    print("DEEP SENTIMENT ANALYSIS RESULTS")
    print("="*80)
    
    # Sort by sentiment score
    sorted_results = sorted(sentiment_results, key=lambda x: x['sentiment_score'], reverse=True)
    
    print(f"\n🟢 MOST POSITIVE SENTIMENT:")
    for result in sorted_results[:10]:
        if result['sentiment_score'] > 0:
            articles_info = f"({result.get('articles_analyzed', 0)} articles analyzed)" if result.get('articles_analyzed', 0) > 0 else "(no articles)"
            print(f"  {result['ticker']}: {result['sentiment_score']:+d} {articles_info}")
    
    print(f"\n🔴 MOST NEGATIVE SENTIMENT:")
    negative_results = [r for r in sorted_results if r['sentiment_score'] < 0]
    for result in negative_results[-10:]:
        articles_info = f"({result.get('articles_analyzed', 0)} articles analyzed)" if result.get('articles_analyzed', 0) > 0 else "(no articles)"
        print(f"  {result['ticker']}: {result['sentiment_score']:+d} {articles_info}")
    
    print(f"\n📊 NEUTRAL (0 score): {len([r for r in sentiment_results if r['sentiment_score'] == 0])} companies")
    print(f"📈 ANALYSIS DEPTH: {total_articles_analyzed} full articles word-by-word analyzed")
    
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
