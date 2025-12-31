#!/usr/bin/env python
"""
国际新闻源数据获取器
支持Reuters RSS、SEC.gov等国际权威源
"""

import json
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import re

try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False
    print("Warning: feedparser not available, RSS support limited")

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("Warning: BeautifulSoup not available, web scraping disabled")

@dataclass
class NewsItem:
    """新闻条目数据结构"""
    title: str
    url: str
    content: str = ""
    summary: str = ""  # 摘要
    full_content: str = ""  # 完整内容
    published: Optional[datetime] = None
    source: str = ""
    categories: List[str] = None
    author: str = ""
    tags: List[str] = None
    
    def __post_init__(self):
        if self.categories is None:
            self.categories = []
        if self.tags is None:
            self.tags = []

class InternationalNewsAggregator:
    """国际新闻聚合器"""
    
    def __init__(self, config_path: str = "international_sources.json"):
        """初始化聚合器"""
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['settings']['user_agent']
        })
        
    def _load_config(self) -> dict:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Config file {self.config_path} not found")
            return {"international_sources": {}, "settings": {}}
    
    def fetch_rss_feed(self, feed_url: str, timeout: int = 30) -> List[NewsItem]:
        """获取RSS源数据"""
        if not FEEDPARSER_AVAILABLE:
            print(f"RSS support not available, skipping {feed_url}")
            return []
            
        try:
            print(f"Fetching RSS: {feed_url}")
            
            # 设置feedparser的用户代理
            feedparser.USER_AGENT = self.config['settings']['user_agent']
            
            # 获取RSS内容
            feed = feedparser.parse(feed_url)
            
            if feed.bozo:
                print(f"RSS feed may have issues: {feed_url}")
            
            items = []
            max_items = self.config['settings']['max_articles_per_source'] * 2  # 增加数量
            
            for entry in feed.entries[:max_items]:
                # 解析发布时间
                published = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    published = datetime(*entry.published_parsed[:6])
                elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                    published = datetime(*entry.updated_parsed[:6])
                
                # 获取内容 - 增强版本
                content = ""
                summary = ""
                
                if hasattr(entry, 'content') and entry.content:
                    # 优先使用完整内容
                    if isinstance(entry.content, list):
                        content = entry.content[0].value if entry.content else ""
                    else:
                        content = str(entry.content)
                
                if hasattr(entry, 'summary'):
                    summary = entry.summary
                elif hasattr(entry, 'description'):
                    summary = entry.description
                
                # 如果content为空，使用summary
                if not content:
                    content = summary
                
                # 清理HTML标签
                if content:
                    content = self._clean_html(content)
                if summary:
                    summary = self._clean_html(summary)
                
                # 尝试获取完整文章内容
                full_content = ""
                if hasattr(entry, 'link') and entry.link:
                    full_content = self._fetch_article_content(entry.link, timeout)
                
                # 获取作者信息
                author = ""
                if hasattr(entry, 'author'):
                    author = entry.author
                elif hasattr(entry, 'authors') and entry.authors:
                    author = entry.authors[0] if entry.authors else ""
                
                # 获取标签
                tags = []
                if hasattr(entry, 'tags') and entry.tags:
                    tags = [tag.term for tag in entry.tags if hasattr(tag, 'term')]
                
                item = NewsItem(
                    title=entry.title if hasattr(entry, 'title') else "",
                    url=entry.link if hasattr(entry, 'link') else "",
                    content=content,
                    summary=summary,
                    full_content=full_content,
                    published=published,
                    source=feed.feed.title if hasattr(feed.feed, 'title') else "",
                    categories=[],
                    author=author,
                    tags=tags
                )
                
                items.append(item)
                print(f"  Found: {item.title[:50]}... (Content: {len(item.full_content or item.content)} chars)")
            
            print(f"Retrieved {len(items)} items from {feed_url}")
            return items
            
        except Exception as e:
            print(f"Error fetching RSS {feed_url}: {e}")
            return []
    
    def fetch_web_content(self, url: str, timeout: int = 30) -> List[NewsItem]:
        """获取网页内容 (简单实现)"""
        if not BS4_AVAILABLE:
            print(f"Web scraping not available, skipping {url}")
            return []
            
        try:
            print(f"Fetching web content: {url}")
            
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 简单的新闻提取逻辑 (可根据具体网站优化)
            items = []
            
            # 查找新闻链接 (通用方法)
            links = soup.find_all('a', href=True)
            news_links = []
            
            for link in links[:50]:  # 限制处理的链接数量
                href = link.get('href')
                text = link.get_text(strip=True)
                
                if (text and len(text) > 20 and 
                    any(keyword in text.lower() for keyword in 
                        ['market', 'stock', 'finance', 'economy', 'business', 'technology'])):
                    
                    if href.startswith('/'):
                        href = requests.urljoin(url, href)
                    
                    news_links.append((text, href))
            
            # 处理找到的新闻
            for title, news_url in news_links[:10]:  # 限制获取数量
                item = NewsItem(
                    title=title,
                    url=news_url,
                    content="",  # 可以进一步获取内容
                    published=datetime.now(),
                    source=url
                )
                items.append(item)
                print(f"  Found: {title[:50]}...")
            
            print(f"Retrieved {len(items)} items from {url}")
            return items
            
        except Exception as e:
            print(f"Error fetching web content {url}: {e}")
            return []
    
    def _fetch_article_content(self, url: str, timeout: int = 15) -> str:
        """抓取文章完整内容"""
        if not BS4_AVAILABLE:
            return ""
        
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 移除脚本和样式
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # 尝试多种内容提取策略
            content_selectors = [
                'article',
                '.article-content', 
                '.content',
                '.post-content',
                '.entry-content',
                '.story-body',
                '.article-body',
                '[data-module="ArticleBody"]',
                '.StandardArticleBody_body',
                'div[data-testid="paragraph"]'
            ]
            
            content_text = ""
            
            # 尝试每个选择器
            for selector in content_selectors:
                elements = soup.select(selector)
                if elements:
                    content_text = ' '.join([elem.get_text(strip=True) for elem in elements])
                    if len(content_text) > 200:  # 如果找到足够长的内容就停止
                        break
            
            # 如果没找到，尝试段落标签
            if len(content_text) < 200:
                paragraphs = soup.find_all('p')
                if paragraphs:
                    para_text = ' '.join([p.get_text(strip=True) for p in paragraphs[:10]])
                    if len(para_text) > len(content_text):
                        content_text = para_text
            
            # 清理文本
            content_text = re.sub(r'\s+', ' ', content_text).strip()
            
            # 限制长度
            if len(content_text) > 2000:
                content_text = content_text[:2000] + "..."
            
            return content_text
            
        except Exception as e:
            print(f"  Error fetching content from {url}: {e}")
            return ""
    
    def _clean_html(self, text: str) -> str:
        """清理HTML标签"""
        if not text:
            return ""
        
        # 简单的HTML标签清理
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'&[^;]+;', '', text)  # 移除HTML实体
        return text.strip()
    
    def fetch_all_sources(self) -> Dict[str, List[NewsItem]]:
        """获取所有启用源的数据"""
        all_items = {}
        
        sources = self.config.get('international_sources', {})
        
        for source_id, source_config in sources.items():
            if not source_config.get('enabled', True):
                print(f"Skipping disabled source: {source_id}")
                continue
            
            print(f"\\nProcessing source: {source_config['name']}")
            source_items = []
            
            source_type = source_config.get('type', 'rss')
            feeds = source_config.get('feeds', [])
            timeout = source_config.get('timeout', self.config['settings']['default_timeout'])
            
            for feed in feeds:
                feed_url = feed['url']
                
                if source_type == 'rss':
                    items = self.fetch_rss_feed(feed_url, timeout)
                elif source_type == 'web':
                    items = self.fetch_web_content(feed_url, timeout)
                elif source_type == 'api':
                    print(f"API support for {source_id} not yet implemented")
                    continue
                else:
                    print(f"Unknown source type: {source_type}")
                    continue
                
                # 添加源信息和分类信息
                for item in items:
                    item.source = f"{source_config['name']} - {feed['name']}"
                    if 'sectors' in feed:
                        item.categories.extend(feed['sectors'])
                
                source_items.extend(items)
                
                # 添加延迟避免过于频繁的请求
                time.sleep(1)
            
            all_items[source_id] = source_items
            print(f"Source {source_id}: {len(source_items)} total items")
        
        return all_items
    
    def save_to_files(self, items_by_source: Dict[str, List[NewsItem]], 
                     output_dir: str = "output") -> List[str]:
        """保存到文件，使用市场+行业分类"""
        try:
            from topic_classifier import classify_market_and_sector
            CLASSIFICATION_AVAILABLE = True
        except ImportError:
            print("Warning: topic_classifier not available, using basic classification")
            CLASSIFICATION_AVAILABLE = False
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
            classified_items = {}  # 用于存储分类的条目
        saved_files = []
        
        # 按分类组织文件
        classified_items = {}
        
        for source_id, items in items_by_source.items():
            for item in items:
                if CLASSIFICATION_AVAILABLE:
                    # 使用现有的分类系统
                    market, sectors = classify_market_and_sector(item.url, item.title, item.content)
                    
                    # 国际源默认归类为INTL市场，但也尝试具体市场识别
                    if market == 'CN':
                        # 国际源中的中国相关新闻
                        market = 'INTL_CN'
                    elif market in ['HK', 'US']:
                        # 保持原有分类，但标记为国际源
                        market = f'INTL_{market}'
                    else:
                        market = 'INTL'
                        
                    if not sectors:
                    # 为每个识别的行业创建条目（使用文章发布日期作为文件日期）
                    for sector in sectors:
                        file_date = (item.published.date().isoformat() if item.published else datetime.now().date().isoformat())
                        key = f"{market}_{sector}_{file_date}.txt"
                        else:
                            sectors = ['general']
                else:
                    # 基础分类
                    market = 'INTL'
                    sectors = item.categories if item.categories else ['general']
                
                # 为每个识别的行业创建条目
                for sector in sectors:
                    key = f"{market}_{sector}_{today}.txt"
                    if key not in classified_items:
                        classified_items[key] = []
                    
                    classified_items[key].append({
                        'title': item.title,
                        'url': item.url,
                        'summary': item.summary,
                        'content': item.full_content or item.content,
                        'published': item.published.isoformat() if item.published else today,
                    # 文件头显示文件日期（从文件名解析或第一条的published）
                    header_date = items[0].get('published', '') if items else ''
                    f.write(f"International News - {header_date}\\n")
                        'author': item.author,
                        'tags': item.tags
                    })
        
        # 写入文件
        for file_name, items in classified_items.items():
            file_path = output_path / file_name
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"International News - {today}\\n")
                f.write("=" * 50 + "\\n\\n")
                
                for item in items:
                    f.write(f"Title: {item['title']}\\n")
                    f.write(f"URL: {item['url']}\\n")
                    f.write(f"Source: {item['source']}\\n")
                    if item.get('author'):
                        f.write(f"Author: {item['author']}\n")
                    f.write(f"Published: {item['published']}\n")
                    if item.get('tags'):
                        f.write(f"Tags: {', '.join(item['tags'])}\n")
                    
                    f.write(f"\nSummary:\n{item.get('summary', 'N/A')}\n\n")
                    
                    content = item.get('content', '')
                    if len(content) > 50:
                        f.write(f"Content:\n{content}\n")
                    else:
                        f.write(f"Content: {content or 'Content not available'}\n")
                    
                    f.write("-" * 50 + "\n\n")
            
            saved_files.append(str(file_path))
            print(f"Saved {len(items)} items to {file_path}")
        
        return saved_files

def main():
    """主函数"""
    print("🌍 International News Aggregator")
    print("=" * 40)
    
    # 检查依赖
    missing_deps = []
    if not FEEDPARSER_AVAILABLE:
        missing_deps.append("feedparser")
    if not BS4_AVAILABLE:
        missing_deps.append("beautifulsoup4")
    
    if missing_deps:
        print(f"\\n⚠️ Missing dependencies: {', '.join(missing_deps)}")
        print("Install with: pip install feedparser beautifulsoup4")
        return
    
    aggregator = InternationalNewsAggregator()
    
    # 获取所有源的数据
    all_items = aggregator.fetch_all_sources()
    
    if not all_items:
        print("\\n❌ No items retrieved from any source")
        return
    
    # 保存到文件
    print("\\n💾 Saving to files...")
    saved_files = aggregator.save_to_files(all_items)
    
    print(f"\\n✅ Completed! Saved {len(saved_files)} files:")
    for file_path in saved_files:
        print(f"   📄 {file_path}")

if __name__ == "__main__":
    main()