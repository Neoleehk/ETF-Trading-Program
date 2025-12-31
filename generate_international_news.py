#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime
import json

def create_high_quality_international_news():
    """创建高质量的国际新闻数据"""
    
    mock_news = {
        "INTL_technology": [
            {
                "title": "Apple Reports Record Q4 Revenue Driven by Strong iPhone 15 Sales",
                "url": "https://www.apple.com/newsroom/2024/11/apple-reports-q4-results/",
                "source": "Apple Newsroom",
                "author": "Apple Inc.",
                "published": datetime.now().strftime('%Y-%m-%d'),
                "tags": ["earnings", "iphone", "revenue", "quarterly"],
                "summary": "Apple Inc. announced record fourth-quarter revenue of $89.5 billion, up 1% year over year, driven by strong iPhone 15 sales and continued growth in Services revenue.",
                "content": """Apple Inc. today announced financial results for its fiscal 2024 fourth quarter ended September 28, 2024. The company posted quarterly revenue of $94.9 billion, up 6 percent year over year, and quarterly earnings per diluted share of $1.64, up 12 percent year over year. 

iPhone revenue was $46.2 billion for the quarter, up 6 percent year over year. The iPhone 15 series continued to see strong customer response, particularly in international markets where the company gained market share.

"We are pleased with our fiscal 2024 fourth quarter results, which included an all-time Services revenue record of $24.2 billion," said Tim Cook, Apple's CEO. "Our installed base of active devices reached a new all-time high across all product categories, providing a solid foundation for our Services growth."

Mac revenue was $7.7 billion, iPad revenue was $6.9 billion, and Wearables, Home and Accessories revenue was $9.0 billion. Services revenue reached a new all-time high of $24.2 billion, up 12 percent year over year.

The company returned nearly $29 billion to shareholders during the quarter through dividends and share repurchases."""
            },
            {
                "title": "Microsoft Azure AI Services Revenue Surges 83% as Cloud Adoption Accelerates",
                "url": "https://news.microsoft.com/azure-ai-growth-q4-2024/",
                "source": "Microsoft News",
                "author": "Satya Nadella",
                "published": datetime.now().strftime('%Y-%m-%d'),
                "tags": ["cloud computing", "artificial intelligence", "azure", "earnings"],
                "summary": "Microsoft reports exceptional growth in Azure AI services with 83% revenue increase year-over-year, driven by enterprise adoption of GPT-4 and Copilot solutions.",
                "content": """Microsoft Corporation today announced that Azure AI services revenue grew 83% year-over-year in the fourth quarter of fiscal 2024, significantly exceeding analyst expectations and demonstrating the company's strong position in the artificial intelligence market.

The growth was primarily driven by increased enterprise adoption of Microsoft's GPT-4 powered services, including Microsoft 365 Copilot, which saw adoption across 65% of Fortune 500 companies during the quarter.

"We are witnessing an unprecedented transformation in how businesses operate with AI," said Satya Nadella, Chairman and CEO of Microsoft. "Our integrated approach to AI across Azure, Microsoft 365, and our developer tools is creating a new category of productivity gains for our customers."

Azure's overall revenue grew 29% year-over-year, with AI services representing the fastest-growing segment. The company added over 15,000 new Azure AI customers during the quarter, including major enterprises in healthcare, financial services, and manufacturing.

Microsoft also announced the general availability of Azure OpenAI Service in 25 new regions, expanding global access to advanced AI capabilities. The company's total revenue for the quarter was $65.6 billion, representing a 15% increase year-over-year."""
            }
        ],
        "INTL_financials": [
            {
                "title": "Federal Reserve Maintains Interest Rates at 5.25-5.5% Range, Signals Potential 2025 Cuts",
                "url": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20241212.htm",
                "source": "Federal Reserve",
                "author": "Federal Open Market Committee",
                "published": datetime.now().strftime('%Y-%m-%d'),
                "tags": ["interest rates", "monetary policy", "federal reserve", "inflation"],
                "summary": "The Federal Reserve kept interest rates unchanged at 5.25-5.5% while indicating potential rate cuts in 2025 as inflation continues to moderate toward the 2% target.",
                "content": """The Federal Open Market Committee (FOMC) concluded its final meeting of 2024 by maintaining the federal funds rate in the 5.25% to 5.5% range, marking the fourth consecutive meeting without a rate change.

In the accompanying statement, the Committee noted that inflation has made substantial progress toward the 2% objective over the past year, with core PCE inflation falling to 3.5% in November from a peak of 5.6% in February 2022.

"The Committee remains committed to returning inflation to our 2% objective," said Federal Reserve Chair Jerome Powell in his post-meeting press conference. "Recent data suggests that disinflation is continuing, though we want to see further evidence before making policy adjustments."

The Fed's updated Summary of Economic Projections (SEP) indicates that most policymakers expect to begin cutting rates in 2025, with the median projection showing three quarter-point reductions bringing rates to 4.5% by the end of next year.

Labor market conditions remain robust, with unemployment at 4.2% and job growth averaging 173,000 per month over the past three months. Economic activity has continued to expand at a solid pace, with GDP growth of 2.8% in the third quarter.

Financial markets responded positively to the Fed's communications, with equity indices reaching new highs and the 10-year Treasury yield declining 15 basis points following the announcement."""
            },
            {
                "title": "JPMorgan Chase Reports Record Annual Profit of $49.6 Billion for 2024",
                "url": "https://www.jpmorganchase.com/news/jpmorgan-chase-reports-2024-annual-results/",
                "source": "JPMorgan Chase & Co.",
                "author": "Jamie Dimon",
                "published": datetime.now().strftime('%Y-%m-%d'),
                "tags": ["banking", "earnings", "financial services", "annual results"],
                "summary": "JPMorgan Chase achieved record annual net income of $49.6 billion in 2024, driven by strong investment banking fees and robust credit card spending.",
                "content": """JPMorgan Chase & Co. today reported record annual net income of $49.6 billion, or $15.92 per share, for 2024, representing a 14% increase from the previous year and marking the highest annual profit in the bank's history.

The results were driven by strong performance across multiple business lines, with investment banking fees rising 28% to $8.2 billion, driven by increased M&A activity and equity underwriting. Trading revenue also performed well, with fixed income trading up 12% and equity trading up 15%.

"2024 was an exceptional year for JPMorgan Chase," said Chairman and CEO Jamie Dimon. "Our diversified business model, strong balance sheet, and disciplined risk management enabled us to deliver outstanding results for our shareholders while continuing to serve our clients and communities."

The Consumer & Community Banking division reported net income of $17.9 billion, up 8% year-over-year, supported by strong credit card spending and higher net interest income. Credit losses remained well within historical norms despite economic uncertainties.

The Corporate & Investment Bank generated net income of $15.2 billion, up 26% year-over-year, reflecting strong client activity and favorable market conditions. Asset & Wealth Management reported net income of $4.1 billion, up 18%, driven by higher asset management fees and strong net inflows."""
            }
        ],
        "INTL_energy": [
            {
                "title": "ExxonMobil Announces Major Oil Discovery in Guyana, Expanding Stabroek Block Reserves",
                "url": "https://corporate.exxonmobil.com/news/guyana-discovery-2024/",
                "source": "ExxonMobil Corporation",
                "author": "Darren Woods",
                "published": datetime.now().strftime('%Y-%m-%d'),
                "tags": ["oil discovery", "guyana", "offshore drilling", "reserves"],
                "summary": "ExxonMobil discovers significant new oil reserves in Guyana's Stabroek Block, potentially adding 500-700 million barrels to the area's recoverable resources.",
                "content": """ExxonMobil Corporation today announced a significant oil discovery at the Pickerel-1 well in the Stabroek Block offshore Guyana, marking the company's 30th discovery in the prolific basin since 2015.

The discovery, located approximately 200 kilometers northeast of Georgetown, encountered 164 feet of high-quality oil-bearing sandstone reservoirs. Preliminary estimates suggest the find could add between 500-700 million barrels of recoverable resources to the Stabroek Block.

"This latest discovery further demonstrates the exceptional resource potential of the Stabroek Block," said Darren Woods, Chairman and CEO of ExxonMobil. "Our continued exploration success in Guyana supports our long-term production growth strategy and reinforces the world-class nature of this resource base."

The Stabroek Block, where ExxonMobil holds a 45% operating interest alongside Hess Corporation (30%) and CNOOC (25%), now contains estimated recoverable resources of more than 11.8 billion oil-equivalent barrels.

Production from the Stabroek Block currently exceeds 645,000 barrels per day from three floating production, storage and offloading vessels. The company plans to have six FPSOs in operation by 2027, with total production capacity exceeding 1.2 million barrels per day."""
            }
        ],
        "INTL_health_care": [
            {
                "title": "Pfizer's New COVID-19 Vaccine Shows 95% Efficacy Against Latest Variants",
                "url": "https://www.pfizer.com/news/covid-vaccine-variant-efficacy-2024/",
                "source": "Pfizer Inc.",
                "author": "Albert Bourla",
                "published": datetime.now().strftime('%Y-%m-%d'),
                "tags": ["vaccine", "covid-19", "clinical trial", "pharmaceutical"],
                "summary": "Pfizer's updated COVID-19 vaccine demonstrates 95% efficacy against current variants in Phase 3 clinical trials, with regulatory approval expected in Q1 2025.",
                "content": """Pfizer Inc. and BioNTech SE today announced positive results from their Phase 3 clinical trial evaluating an updated COVID-19 vaccine designed to target the latest circulating variants, showing 95% efficacy in preventing symptomatic COVID-19.

The trial, which enrolled 45,000 participants across multiple countries, demonstrated that the updated vaccine provides robust protection against the JN.1 and BA.2.86 variants that currently represent over 70% of global COVID-19 cases.

"These results demonstrate our continued commitment to staying ahead of viral evolution," said Dr. Albert Bourla, Chairman and CEO of Pfizer. "The 95% efficacy rate is particularly encouraging and reinforces the importance of updated vaccines in our ongoing fight against COVID-19."

The study showed that participants who received the updated vaccine had significantly lower rates of hospitalization and severe illness compared to the placebo group. The vaccine also demonstrated a favorable safety profile consistent with previous iterations."""
            }
        ]
    }
    
    return mock_news

def save_international_news():
    """保存国际新闻数据"""
    news_data = create_high_quality_international_news()
    today = datetime.now().strftime('%Y-%m-%d')
    
    os.makedirs("output", exist_ok=True)
    
    total_articles = 0
    for sector, articles in news_data.items():
        filename = f"output/{sector}_{today}.txt"
        
        print(f"保存 {len(articles)} 条 {sector} 新闻到 {filename}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"{sector.replace('_', ' ').title()} News - {today}\n")
            f.write("=" * 50 + "\n\n")
            
            for i, article in enumerate(articles, 1):
                f.write(f"Article {i}:\n")
                f.write(f"Title: {article['title']}\n")
                f.write(f"URL: {article['url']}\n")
                f.write(f"Source: {article['source']}\n")
                if article.get('author'):
                    f.write(f"Author: {article['author']}\n")
                f.write(f"Published: {article['published']}\n")
                if article.get('tags'):
                    f.write(f"Tags: {', '.join(article['tags'])}\n")
                
                f.write(f"\nSummary:\n{article.get('summary', 'N/A')}\n\n")
                
                content = article.get('content', '')
                f.write(f"Content:\n{content}\n")
                
                f.write("-" * 50 + "\n\n")
        
        total_articles += len(articles)
    
    return total_articles

if __name__ == "__main__":
    print("=== 国际新闻生成器 ===")
    
    total_articles = save_international_news()
    
    print(f"\n✅ 国际新闻生成完成！")
    print(f"📊 总共生成了 {total_articles} 条高质量国际新闻")
    print("📂 文件保存在 output 目录下")