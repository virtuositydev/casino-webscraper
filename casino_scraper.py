#!/usr/bin/env python3

import re
import json
import hashlib
import logging
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import time
import os
import unicodedata

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# For City of Dreams (requires Playwright)
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("Warning: Playwright not installed. City of Dreams scraping will be limited.")
    print("Install with: pip install playwright && playwright install chromium")

# -------------------------------
# CONFIG
# -------------------------------
CASINOS = {
    "city_of_dreams": {
        "base_url": "https://www.cityofdreamsmanila.com",
        "offers_url": "https://www.cityofdreamsmanila.com/en/offers",
        "additional_pages": [
            "https://www.cityofdreamsmanila.com/en/meetings-and-events",
            "https://www.cityofdreamsmanila.com/en/whats-on",
            "https://www.cityofdreamsmanila.com/en/play",
            "https://www.cityofdreamsmanila.com/en/stay",
            "https://www.cityofdreamsmanila.com/en/dine",
            "https://www.cityofdreamsmanila.com/en/enjoy"
        ],
        "requires_js": True
    },
    "newport_world": {
        "base_url": "https://www.newportworldresorts.com",
        "potential_promo_pages": [
            "/", "/promotions", "/offers", "/deals", "/packages", "/specials",
            "/events", "/whats-on", "/happenings", "/hotels", "/accommodation",
            "/stay", "/rooms", "/hotels/promotions", "/hotels/offers",
            "/dining", "/restaurants", "/food-and-beverage", "/dining/promotions",
            "/gaming", "/casino", "/gaming/promotions", "/player-rewards",
            "/entertainment", "/shows", "/concerts", "/shopping", "/spa", 
            "/membership", "/rewards", "/loyalty"
        ],
        "requires_js": False
    },
    "okada": {
        "base_url": "https://okadamanila.com",
        "category_urls": [
            "https://okadamanila.com/deals-category/special-promo",
            "https://okadamanila.com/deals-category/reward-circle-promo",
            "https://okadamanila.com/deals-category/stay-deal",
            "https://okadamanila.com/deals-category/dine-deal"
        ],
        "requires_js": False
    },
    "solaire": {
        "base_url": "https://sec.solaireresort.com",
        "offers_url": "https://sec.solaireresort.com/offers",
        "additional_pages": [
            "/dining", "/entertainment", "/gaming", "/hotel", "/spa"
        ],
        "requires_js": True
    }
}

JACKPOT_CONFIG = {
    "city_of_dreams": {
        "url": "https://www.cityofdreamsmanila.com/en/whats-on/slot-grand-jackpots",
        "requires_js": True
    },
    "solaire": {
        "url": "https://sec.solaireresort.com/gaming/billboard",
        "requires_js": True
    }
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

# Generate filename with current date
current_date = datetime.now().strftime("%Y%m%d")
OUTPUT_JSON = f"manila_casinos_promos_{current_date}.json"

REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.0
STATUS_FORCELIST = (429, 500, 502, 503, 504)

# Scraping limits (set to None for unlimited)
MAX_PAGES_PER_CASINO = None

# -------------------------------
# Logging
# -------------------------------
logger = logging.getLogger("unified_scraper")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)

# -------------------------------
# HTTP session with retries
# -------------------------------
def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=MAX_RETRIES,
        read=MAX_RETRIES,
        connect=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=STATUS_FORCELIST,
        allowed_methods=["GET", "POST", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

session = create_session()


def get_category_from_url(url: str, casino_name: str) -> str:
    """Determine category based on URL pattern"""
    url_lower = url.lower()
    
    if any(x in url_lower for x in ['/dining', 'restaurant', '/food', 'dine-deal', '/dine']):
        return "Dining"
    elif any(x in url_lower for x in ['/hotel', '/stay', 'room', 'suite', '/accommodation', 'stay-deal']):
        return "Hotel & Accommodations"
    elif any(x in url_lower for x in ['/gaming', 'casino', 'poker', '/play']):
        return "Gaming & Casino"
    elif any(x in url_lower for x in ['/entertainment', 'show', 'concert', 'event', '/enjoy']):
        return "Entertainment"
    elif any(x in url_lower for x in ['/shopping', '/shop', '/retail']):
        return "Shopping"
    elif any(x in url_lower for x in ['/spa', 'wellness', 'massage']):
        return "Spa & Wellness"
    elif any(x in url_lower for x in ['/meetings-and-events']):
        return "Meetings & Events"
    elif '/whats-on' in url_lower:
        return "What's On"
    elif 'reward-circle' in url_lower:
        return "Reward Circle"
    elif any(x in url_lower for x in ['/promotion', '/offer', '/deal', '/package', 'special-promo']):
        return "Special Offers"
    else:
        return "Other"


def is_promotional_content(text: str, url: str = "") -> bool:
    """Check if content appears to be promotional"""
    # SPECIAL CASE: If URL contains /offers/, it's definitely promotional
    if "/offers/" in url:
        return True
    
    promo_keywords = [
        'promotion', 'offer', 'deal', 'discount', 'sale', 'special',
        'package', 'promo', 'save', 'free', 'complimentary', 'bonus',
        'reward', 'exclusive', 'limited time', 'book now', 'reserve',
        '% off', 'percent off', 'buy one', 'get one', 'upgrade'
    ]
    
    text_lower = text.lower()
    url_lower = url.lower()
    
    keyword_count = sum(1 for keyword in promo_keywords if keyword in text_lower or keyword in url_lower)
    return keyword_count >= 1  # Changed from 2 to 1 for more lenient check

def extract_title(soup: BeautifulSoup) -> str:
    """Extract page title"""
    for selector in ["h1", "h2.page-title", ".entry-title", ".promo-title", "title"]:
        elem = soup.select_one(selector)
        if elem:
            title = elem.get_text(strip=True)
            if selector == "title":
                title = re.sub(r'\s*[-|]\s*(City of Dreams|Newport|Okada|Solaire).*$', '', title, flags=re.IGNORECASE)
            if title:
                return title
    return "Untitled"

def clean_raw_content(soup: BeautifulSoup, raw_text: str) -> str:
    """
    Clean raw content by removing header, footer, navigation, and other non-essential elements
    
    Args:
        soup: BeautifulSoup object of the page
        raw_text: Raw text content extracted from the page
    
    Returns:
        Cleaned text content
    """
    # Remove common header/footer/navigation elements from soup before extraction
    elements_to_remove = [
        'header', 'footer', 'nav',
        '[class*="header"]', '[class*="Header"]',
        '[class*="footer"]', '[class*="Footer"]',
        '[class*="navigation"]', '[class*="nav-"]',
        '[class*="menu"]', '[class*="Menu"]',
        '[id*="header"]', '[id*="Header"]',
        '[id*="footer"]', '[id*="Footer"]',
        '[id*="navigation"]', '[id*="nav"]',
        '[id*="menu"]', '[id*="Menu"]',
        '.site-header', '.site-footer',
        '#site-header', '#site-footer',
        'aside', '[role="navigation"]',
        '[role="banner"]', '[role="contentinfo"]',
        '.breadcrumb', '.breadcrumbs',
        '.social-links', '.social-media',
        'script', 'style', 'noscript'
    ]
    
    for selector in elements_to_remove:
        for element in soup.select(selector):
            element.decompose()
    
    # Also remove common patterns from text
    # Split into lines for line-by-line filtering
    lines = raw_text.split('\n')
    cleaned_lines = []
    
    # Patterns to skip (common navigation/footer text)
    skip_patterns = [
        r'^(what\'?s on|play|stay|dine|enjoy|shop|events)$',
        r'^(home|about us|contact us|careers|getting here)$',
        r'^(media center|sustainability|responsible gaming)$',
        r'^(privacy policy|terms and conditions|cookie policy)$',
        r'^\+?\d{2,4}\s*\d{4}\s*\d{4}$',  # Phone numbers alone
        r'^(book now|sign up|login|register)$',
        r'^(eng|中文|한국어|日本語)$',  # Language selectors
        r'^view all$',
        r'^©\s*copyright',
        r'^follow us:?$',
        r'^connect with us:?$',
        r'we use cookies',
        r'^newsletter$',
        r'^back to top$',
        r'^menu$' if len(lines) > 10 else None,  # Only skip single "MENU" if it's likely navigation
    ]
    
    for line in lines:
        line_stripped = line.strip()
        
        # Skip empty lines
        if not line_stripped:
            continue
        
        # Skip lines matching skip patterns
        should_skip = False
        for pattern in skip_patterns:
            if pattern and re.match(pattern, line_stripped, re.IGNORECASE):
                should_skip = True
                break
        
        if not should_skip:
            cleaned_lines.append(line_stripped)
    
    # Rejoin and clean up extra whitespace
    cleaned_text = ' '.join(cleaned_lines)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    
    # Remove common footer text patterns from the full text
    footer_patterns = [
        r'©\s*COPYRIGHT\s+\d{4}.*?(?=\w{3,}|\Z)',
        r'Privacy Policy\s+Terms and Conditions.*?(?=\w{3,}|\Z)',
        r'We use cookies on this site.*?(?=\w{3,}|\Z)',
        r'FOLLOW US:.*?(?=\w{3,}|\Z)',
        r'CONNECT WITH US:.*?(?=\w{3,}|\Z)',
    ]
    
    for pattern in footer_patterns:
        cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE | re.DOTALL)
    
    # Final cleanup
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    
    return cleaned_text


def scrape_page_static(url: str, casino_name: str) -> Optional[Dict[str, Any]]:
    """Scrape a page using BeautifulSoup (for static content)"""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        title = extract_title(soup)
        
        # Find main content area
        main_content = None
        content_selectors = [
            "article", "div.content", "div.main-content", "div.entry-content",
            "div.post-content", "div.page-content", "main", "div#content", 
            "div.container", "div.offer-detail", "div.promo-detail"
        ]
        
        for selector in content_selectors:
            elem = soup.select_one(selector)
            if elem:
                main_content = elem
                break
        
        if not main_content:
            main_content = soup.find("body") or soup

        # Create a copy for cleaning
        content_copy = BeautifulSoup(str(main_content), 'html.parser')
        
        raw_content = clean_raw_content(content_copy, content_copy.get_text(" ", strip=True))
        
        # Skip if not promotional content
        if not is_promotional_content(raw_content, url):
            return None
        
        item_id = hashlib.sha256(url.encode()).hexdigest()
        category = get_category_from_url(url, casino_name)
        
        return {
            "id": item_id,
            "casino": casino_name,
            "title": title,
            "url": url,
            "category": category,
            "raw_content": raw_content[:5000],
            "scraped_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None


# -------------------------------
# Casino-specific scrapers
# -------------------------------

def scrape_city_of_dreams():
    """Enhanced scraper for City of Dreams Manila"""
    logger.info("\n=== Scraping City of Dreams Manila ===")
    results = []
    
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not available. Using fallback scraping method...")
        # Try basic scraping with additional selectors
        all_links = set()
        
        for page_url in [CASINOS["city_of_dreams"]["offers_url"]] + CASINOS["city_of_dreams"]["additional_pages"]:
            try:
                resp = session.get(page_url, timeout=REQUEST_TIMEOUT)
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # Multiple selector strategies
                link_selectors = [
                    "a[href*='/offers/']",
                    "a[href*='/en/offers/']",
                    "a[href*='/whats-on/']",
                    "a[href*='/en/whats-on/']",
                    "a[href*='/promotions/']",
                    "a[href*='/deals/']",
                    ".offer-card a",
                    ".promo-card a",
                    ".card a[href]",
                    "article a[href]"
                ]
                
                for selector in link_selectors:
                    for a in soup.select(selector):
                        href = a.get("href", "")
                        if href and not href.startswith("#"):
                            full_url = urljoin(page_url, href)
                            # Filter out category pages
                            if not any(full_url.rstrip('/').endswith(x) for x in ['/offers', '/en/offers', '/whats-on', '/en/whats-on']):
                                all_links.add(full_url)
                
            except Exception as e:
                logger.error(f"Error fetching {page_url}: {e}")
        
        logger.info(f"Found {len(all_links)} potential offer links using fallback method")
        
        # Scrape the links
        for url in list(all_links)[:MAX_PAGES_PER_CASINO] if MAX_PAGES_PER_CASINO else all_links:
            result = scrape_page_static(url, "City of Dreams Manila")
            if result:
                results.append(result)
                logger.info(f"  Scraped: {result['title']}")
        
        return results
    
    # Full Playwright scraping with better selectors and wait strategies
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            all_offer_links = set()
            
            # Scrape main offers page and additional pages
            pages_to_check = [CASINOS["city_of_dreams"]["offers_url"]] + CASINOS["city_of_dreams"]["additional_pages"]
            
            for page_url in pages_to_check:
                logger.info(f"  Checking: {page_url}")
                try:
                    page.goto(page_url, wait_until='networkidle', timeout=60000)
                    page.wait_for_timeout(3000)
                    
                    # Scroll to trigger lazy loading
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(2000)
                    
                    # Multiple extraction methods
                    # Method 1: JavaScript evaluation
                    js_links = page.evaluate("""
                        () => {
                            const links = new Set();
                            // Look for all links that might be offers
                            document.querySelectorAll('a[href]').forEach(link => {
                                const href = link.href;
                                if (href && (
                                    href.includes('/offers/') ||
                                    href.includes('/en/offers/') ||
                                    href.includes('/whats-on/') ||
                                    href.includes('/en/whats-on/') ||
                                    href.includes('/play/') ||
                                    href.includes('/stay/') ||
                                    href.includes('/dine/') ||
                                    href.includes('/enjoy/')
                                )) {
                                    // Filter out category pages
                                    if (!href.endsWith('/offers') && 
                                        !href.endsWith('/en/offers') &&
                                        !href.endsWith('/whats-on') &&
                                        !href.endsWith('/en/whats-on')) {
                                        links.add(href);
                                    }
                                }
                            });
                            return Array.from(links);
                        }
                    """)
                    
                    for link in js_links:
                        all_offer_links.add(link)
                    
                    # Method 2: Playwright selectors
                    offer_elements = page.locator('a[href*="/offers/"], a[href*="/whats-on/"]').all()
                    for elem in offer_elements:
                        try:
                            href = elem.get_attribute('href')
                            if href:
                                full_url = urljoin(page_url, href)
                                if not any(full_url.rstrip('/').endswith(x) for x in ['/offers', '/en/offers', '/whats-on']):
                                    all_offer_links.add(full_url)
                        except:
                            continue
                    
                except PlaywrightTimeoutError:
                    logger.error(f"  Timeout loading {page_url}")
                except Exception as e:
                    logger.error(f"  Error loading {page_url}: {e}")
            
            logger.info(f"Found {len(all_offer_links)} offer links")
            
            # Scrape each offer page
            offer_list = list(all_offer_links)[:MAX_PAGES_PER_CASINO] if MAX_PAGES_PER_CASINO else list(all_offer_links)
            
            for i, url in enumerate(offer_list, 1):
                logger.info(f"  Scraping {i}/{len(offer_list)}: {url}")
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    page.wait_for_timeout(2000)

                    html_content = page.content()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    title = page.title()
                    raw_text = clean_raw_content(soup, page.inner_text('body'))
                    
                    # Check if promotional content
                    if not is_promotional_content(raw_text, url):
                        continue
                    
                    item_id = hashlib.sha256(url.encode()).hexdigest()
                    category = get_category_from_url(url, "City of Dreams Manila")
                    
                    results.append({
                        "id": item_id,
                        "casino": "City of Dreams Manila",
                        "title": title,
                        "url": url,
                        "category": category,
                        "raw_content": raw_text[:5000],
                        "scraped_at": datetime.now(timezone.utc).isoformat()
                    })
                    
                    logger.info(f"    Scraped: {title}")
                    
                except PlaywrightTimeoutError:
                    logger.error(f"    Timeout scraping {url}")
                except Exception as e:
                    logger.error(f"    Error scraping {url}: {e}")
                
                # Small delay between requests
                time.sleep(0.5)
            
            browser.close()
            
    except Exception as e:
        logger.error(f"Failed to initialize Playwright: {e}")
        logger.info("Falling back to static scraping...")
        # Fallback to static scraping
        return scrape_city_of_dreams_static()
    
    logger.info(f"  Total scraped from City of Dreams: {len(results)}")
    return results


def scrape_city_of_dreams_static():
    """Static fallback scraper for City of Dreams"""
    results = []
    all_links = set()
    
    # Try to get links from all pages
    pages_to_check = [CASINOS["city_of_dreams"]["offers_url"]] + CASINOS["city_of_dreams"]["additional_pages"]
    
    for page_url in pages_to_check:
        try:
            resp = session.get(page_url, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Look for all possible offer links
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if any(pattern in href for pattern in ['/offers/', '/whats-on/', '/play/', '/stay/', '/dine/', '/enjoy/']):
                    full_url = urljoin(page_url, href)
                    if not any(full_url.rstrip('/').endswith(x) for x in ['/offers', '/whats-on', '/play', '/stay', '/dine', '/enjoy']):
                        all_links.add(full_url)
        except Exception as e:
            logger.error(f"Error fetching {page_url}: {e}")
    
    logger.info(f"Found {len(all_links)} links via static method")
    
    # Scrape each link
    for url in list(all_links)[:MAX_PAGES_PER_CASINO] if MAX_PAGES_PER_CASINO else all_links:
        result = scrape_page_static(url, "City of Dreams Manila")
        if result:
            results.append(result)
            logger.info(f"  Scraped: {result['title']}")
    
    return results

# Add these functions after scrape_city_of_dreams_static() in your script:

def scrape_newport_world():
    """Enhanced scraper for Newport World Resorts"""
    logger.info("\n=== Scraping Newport World Resorts ===")
    
    all_links = set()
    base_url = CASINOS["newport_world"]["base_url"]
    
    # Check all potential promotional pages
    for page_path in CASINOS["newport_world"]["potential_promo_pages"]:
        page_url = base_url if page_path == "/" else base_url + page_path
        
        try:
            resp = session.get(page_url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # Look for all links that might be promotional
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    link_text = a.get_text(strip=True).lower()
                    
                    if not href or href.startswith("#") or href.startswith("javascript:"):
                        continue
                    
                    full_url = urljoin(page_url, href)
                    
                    # Check if URL is within Newport domain
                    if urlparse(full_url).netloc != urlparse(base_url).netloc:
                        continue
                    
                    # Get parent text for context
                    parent_text = ""
                    if a.parent:
                        parent_text = a.parent.get_text(strip=True).lower()
                    
                    # Check if promotional content
                    if is_promotional_content(link_text + " " + parent_text, href):
                        all_links.add(full_url)
                    
                    # Also check URL patterns
                    if any(pattern in href.lower() for pattern in 
                           ['promo', 'offer', 'deal', 'package', 'special', 'reward', 'event']):
                        all_links.add(full_url)
                        
        except Exception as e:
            logger.error(f"  Error checking {page_url}: {e}")
            continue
    
    logger.info(f"Found {len(all_links)} potential promotional links")
    
    results = []
    links_to_scrape = list(all_links)[:MAX_PAGES_PER_CASINO] if MAX_PAGES_PER_CASINO else list(all_links)
    
    for i, url in enumerate(links_to_scrape, 1):
        logger.info(f"  Scraping {i}/{len(links_to_scrape)}: {url}")
        result = scrape_page_static(url, "Newport World Resorts")
        if result:
            results.append(result)
            logger.info(f"    Scraped: {result['title']}")
        time.sleep(0.3)  # Small delay between requests
    
    logger.info(f"  Total scraped from Newport: {len(results)}")
    return results


def scrape_okada():
    """Enhanced scraper for Okada Manila"""
    logger.info("\n=== Scraping Okada Manila ===")
    
    all_links = set()
    
    # Get links from all category pages
    for category_url in CASINOS["okada"]["category_urls"]:
        logger.info(f"  Checking category: {category_url}")
        try:
            resp = session.get(category_url, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Look for deal links with multiple selectors
            link_selectors = [
                "a[href*='/deals/']",
                ".deal-card a",
                ".promo-card a", 
                "article a",
                ".entry-content a"
            ]
            
            for selector in link_selectors:
                for a in soup.select(selector):
                    href = a.get("href", "").strip()
                    if href and '/deals/' in href and not href.endswith('/deals/') and not href.endswith('/deals'):
                        full_url = urljoin(category_url, href)
                        if 'okadamanila.com' in full_url:
                            all_links.add(full_url)
            
        except Exception as e:
            logger.error(f"  Error fetching {category_url}: {e}")
    
    logger.info(f"Found {len(all_links)} promotional links")
    
    results = []
    links_to_scrape = list(all_links)[:MAX_PAGES_PER_CASINO] if MAX_PAGES_PER_CASINO else list(all_links)
    
    for i, url in enumerate(links_to_scrape, 1):
        logger.info(f"  Scraping {i}/{len(links_to_scrape)}: {url}")
        result = scrape_page_static(url, "Okada Manila")
        if result:
            results.append(result)
            logger.info(f"    Scraped: {result['title']}")
        time.sleep(0.3)  # Small delay
    
    logger.info(f"  Total scraped from Okada: {len(results)}")
    return results


def scrape_solaire():
    """Enhanced scraper for Solaire Resort with Playwright support"""
    logger.info("\n=== Scraping Solaire Resort ===")
    results = []
    
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not available. Using fallback scraping method...")
        return scrape_solaire_static()
    
    # Full Playwright scraping
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            all_offer_links = set()
            base_url = CASINOS["solaire"]["base_url"]
            offers_url = CASINOS["solaire"]["offers_url"]
            
            # Pages to check
            pages_to_check = [offers_url]
            for page_path in CASINOS["solaire"]["additional_pages"]:
                pages_to_check.append(base_url + page_path)
            
            for page_url in pages_to_check:
                logger.info(f"  Checking: {page_url}")
                try:
                    page.goto(page_url, wait_until='networkidle', timeout=60000)
                    page.wait_for_timeout(3000)  # Wait for JavaScript to render
                    
                    # Scroll to trigger lazy loading
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(2000)
                    
                    # Extract links using JavaScript
                    js_links = page.evaluate("""
                        () => {
                            const links = new Set();
                            document.querySelectorAll('a[href]').forEach(link => {
                                const href = link.href;
                                if (href && href.includes('/offers/')) {
                                    // Filter out language versions and base pages
                                    if (!href.includes('/zh-hans/') && 
                                        !href.includes('/kr/') && 
                                        !href.includes('/jp/') &&
                                        !href.endsWith('/offers') &&
                                        !href.endsWith('/offers/')) {
                                        links.add(href);
                                    }
                                }
                            });
                            return Array.from(links);
                        }
                    """)
                    
                    for link in js_links:
                        all_offer_links.add(link)
                        logger.debug(f"    Found: {link}")
                    
                    # Also try Playwright selectors as backup
                    offer_elements = page.locator('a[href*="/offers/"]').all()
                    for elem in offer_elements:
                        try:
                            href = elem.get_attribute('href')
                            if href and not href.startswith('#'):
                                full_url = urljoin(page_url, href)
                                # Skip language versions and base page
                                if (not any(lang in full_url for lang in ['/zh-hans/', '/kr/', '/jp/']) and
                                    full_url.rstrip('/') != offers_url.rstrip('/')):
                                    all_offer_links.add(full_url)
                        except:
                            continue
                    
                    # Look for promotional keywords in links
                    promo_elements = page.locator('a[href*="promo"], a[href*="deal"], a[href*="package"], a[href*="special"]').all()
                    for elem in promo_elements:
                        try:
                            href = elem.get_attribute('href')
                            if href:
                                full_url = urljoin(page_url, href)
                                if base_url in full_url:
                                    all_offer_links.add(full_url)
                        except:
                            continue
                    
                except PlaywrightTimeoutError:
                    logger.error(f"  Timeout loading {page_url}")
                except Exception as e:
                    logger.error(f"  Error loading {page_url}: {e}")
            
            # Remove base URLs and invalid links
            valid_links = []
            skip_urls = [
                base_url.rstrip('/'),
                offers_url.rstrip('/'),
            ]
            
            for link in all_offer_links:
                clean_link = link.rstrip('/')
                if (clean_link not in skip_urls and 
                    not any(skip in link for skip in ['#', '.pdf', '.jpg', '.png', '/page/'])):
                    valid_links.append(link)
            
            logger.info(f"Found {len(valid_links)} valid promotional links")
            
            if valid_links:
                logger.info("  Sample links found:")
                for link in list(valid_links)[:5]:
                    logger.info(f"    - {link}")
            
            # Scrape each offer page
            links_to_scrape = valid_links[:MAX_PAGES_PER_CASINO] if MAX_PAGES_PER_CASINO else valid_links
            
            for i, url in enumerate(links_to_scrape, 1):
                logger.info(f"  Scraping {i}/{len(links_to_scrape)}: {url}")
                try:
                    # Load the offer page
                    page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    page.wait_for_timeout(2000)
                    
                    # Extract title
                    title = "Untitled"
                    try:
                        # Try h1 first
                        h1 = page.locator('h1').first
                        if h1:
                            title = h1.inner_text()
                    except:
                        pass
                    
                    if title == "Untitled":
                        try:
                            title = page.title()
                            title = re.sub(r'\s*[-|]\s*Solaire.*', '', title, flags=re.IGNORECASE)
                        except:
                            pass
                    
                    # Get page content
                    # raw_content = page.inner_text('body')
                    # raw_content = re.sub(r'\s+', ' ', raw_content).strip()
                    html_content = page.content()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Clean the content
                    raw_content = clean_raw_content(soup, page.inner_text('body'))
                    
                    # Check if promotional content
                    if not is_promotional_content(raw_content, url):
                        logger.info(f"    Skipped - Not promotional content")
                        continue
                    
                    # Generate unique ID
                    item_id = hashlib.sha256(url.encode()).hexdigest()
                    category = get_category_from_url(url, "Solaire Resort")
                    
                    results.append({
                        "id": item_id,
                        "casino": "Solaire Resort",
                        "title": title,
                        "url": url,
                        "category": category,
                        "raw_content": raw_content[:5000],
                        "scraped_at": datetime.now(timezone.utc).isoformat()
                    })
                    
                    logger.info(f"    Scraped: {title}")
                    
                except PlaywrightTimeoutError:
                    logger.error(f"    Timeout scraping {url}")
                except Exception as e:
                    logger.error(f"    Error scraping {url}: {e}")
                
                # Small delay between requests
                time.sleep(0.5)
            
            browser.close()
            
    except Exception as e:
        logger.error(f"Failed to initialize Playwright: {e}")
        logger.info("Falling back to static scraping...")
        return scrape_solaire_static()
    
    logger.info(f"  Total scraped from Solaire: {len(results)}")
    return results


def scrape_solaire_static():
    """Static fallback scraper for Solaire Resort"""
    logger.info("  Using static scraping method...")
    results = []
    all_links = set()
    
    base_url = CASINOS["solaire"]["base_url"]
    offers_url = CASINOS["solaire"]["offers_url"]
    
    # Try to get links from all pages
    pages_to_check = [offers_url]
    for page_path in CASINOS["solaire"]["additional_pages"]:
        pages_to_check.append(base_url + page_path)
    
    for page_url in pages_to_check:
        try:
            resp = session.get(page_url, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                continue
                
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Look for offer links with multiple selectors
            link_selectors = [
                "a[href*='/offers/']",
                "a[href*='/promotions/']",
                "a[href*='/deals/']",
                "a[href*='/packages/']",
                "a.offer-link",
                "a.promo-link",
            ]
            
            for selector in link_selectors:
                for a in soup.select(selector):
                    href = a.get("href", "")
                    if href and not href.startswith("#"):
                        full_url = urljoin(page_url, href)
                        # Skip language versions and base page
                        if (not any(lang in full_url for lang in ['/zh-hans/', '/kr/', '/jp/']) and
                            full_url.rstrip('/') != offers_url.rstrip('/')):
                            all_links.add(full_url)
            
            # Also look for promotional keywords
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                text = a.get_text(strip=True).lower()
                
                if any(keyword in href.lower() or keyword in text for keyword in 
                       ['promo', 'deal', 'package', 'special']):
                    full_url = urljoin(page_url, href)
                    if base_url in full_url and full_url.rstrip('/') != offers_url.rstrip('/'):
                        all_links.add(full_url)
                        
        except Exception as e:
            logger.error(f"  Error fetching {page_url}: {e}")
    
    logger.info(f"  Found {len(all_links)} links via static method")
    
    # Scrape each link
    links_to_scrape = list(all_links)[:MAX_PAGES_PER_CASINO] if MAX_PAGES_PER_CASINO else list(all_links)
    
    for i, url in enumerate(links_to_scrape, 1):
        logger.info(f"  Scraping {i}/{len(links_to_scrape)}: {url}")
        result = scrape_page_static(url, "Solaire Resort")
        if result:
            results.append(result)
            logger.info(f"    Scraped: {result['title']}")
        time.sleep(0.3)
    
    logger.info(f"  Total scraped from Solaire: {len(results)}")
    return results

def scrape_city_of_dreams_jackpots():
    """Scrape jackpots from City of Dreams Manila - handles Angular app"""
    logger.info("\n=== Scraping City of Dreams Jackpots ===")
    jackpots = []
    
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not available. Cannot scrape jackpots.")
        return jackpots
    
    url = JACKPOT_CONFIG["city_of_dreams"]["url"]
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            # Track API calls
            api_data = []
            
            def handle_response(response):
                """Capture API responses that might contain jackpot data"""
                if 'api' in response.url.lower() or 'jackpot' in response.url.lower() or 'slot' in response.url.lower():
                    try:
                        if response.status == 200:
                            data = response.json()
                            api_data.append({
                                'url': response.url,
                                'data': data
                            })
                            logger.info(f"  Captured API call: {response.url}")
                    except:
                        pass
            
            page.on('response', handle_response)
            
            logger.info(f"  Loading: {url}")
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=60000)
                # Wait for specific content instead of networkidle
                page.wait_for_selector('body', timeout=10000)
                page.wait_for_timeout(5000)
            except Exception as e:
                logger.warning(f"  Initial load issue: {e}, trying alternative approach")
                # Fallback: try with even more lenient settings
                page.goto(url, wait_until='commit', timeout=30000)
                page.wait_for_timeout(10000)
            
            # Wait for content to load - try multiple wait strategies
            logger.info("  Waiting for content to load...")
            
            # Strategy 1: Wait for table or specific elements
            try:
                page.wait_for_selector('table, [class*="jackpot"], [class*="slot"]', timeout=10000)
                logger.info("  Content elements found")
            except:
                logger.warning("  No table elements found, continuing...")
            
            # Give extra time for Angular to render
            page.wait_for_timeout(8000)
            
            # Scroll to trigger any lazy loading
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            
            # Check if we got API data
            if api_data:
                logger.info(f"  Found {len(api_data)} API responses")
                for api_response in api_data:
                    logger.info(f"  API URL: {api_response['url']}")
                    logger.info(f"  Data sample: {str(api_response['data'])[:200]}")
            
            # Method 1: Try to find a table element
            html_content = page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for table
            tables = soup.find_all('table')
            logger.info(f"  Found {len(tables)} table(s)")
            
            if tables:
                for table in tables:
                    rows = table.find_all('tr')
                    logger.info(f"  Table has {len(rows)} rows")
                    
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            # Check if this looks like jackpot data
                            cell_texts = [cell.get_text(strip=True) for cell in cells]
                            logger.debug(f"    Row: {cell_texts}")
                            
                            # Skip header row
                            if 'GAME' in cell_texts[0].upper() or 'AMOUNT' in cell_texts[0].upper():
                                continue
                            
                            game_name = cell_texts[0]
                            
                            # Look for amount (usually has P or ₱)
                            amount = None
                            min_bet = None
                            
                            for i, cell_text in enumerate(cell_texts):
                                if 'P ' in cell_text or '₱' in cell_text:
                                    if not amount and any(char.isdigit() for char in cell_text):
                                        amount = cell_text
                                    elif amount and not min_bet:
                                        min_bet = cell_text
                            
                            if game_name and amount:
                                amount_numeric = re.sub(r'[^\d,.]', '', amount)
                                
                                jackpots.append({
                                    "casino": "City of Dreams Manila",
                                    "game_name": game_name,
                                    "current_amount": amount,
                                    "amount_numeric": amount_numeric,
                                    "minimum_bet": min_bet or "N/A",
                                    "jackpot_type": "Progressive",
                                    "currency": "PHP",
                                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                                    "source_url": url
                                })
                                logger.debug(f"    Added: {game_name} - {amount}")
            
            # Method 2: If no table, try text parsing
            if len(jackpots) == 0:
                logger.info("  No table found, trying text extraction...")
                page_text = page.inner_text('body')
                
                # Save for debugging
                logger.info(f"  Page text length: {len(page_text)} characters")
                logger.info(f"  First 1000 chars:\n{page_text[:1000]}")
                
                # Look for patterns like: "Game Name    P 12,345,678    P 88"
                lines = page_text.split('\n')
                
                # Try to find structured data
                for i in range(len(lines) - 1):
                    line = lines[i].strip()
                    
                    # Look for lines with currency amounts
                    if ('P ' in line or '₱' in line) and any(char.isdigit() for char in line):
                        # Check if previous line might be game name
                        if i > 0:
                            prev_line = lines[i-1].strip()
                            if prev_line and len(prev_line) < 100 and not any(skip in prev_line.upper() for skip in ['GAME', 'AMOUNT', 'BET', 'COPYRIGHT', 'PRIVACY']):
                                # This might be a game name
                                game_name = prev_line
                                
                                # Extract amounts from current line
                                amounts = re.findall(r'[P₱]\s*[\d,]+', line)
                                if len(amounts) >= 1:
                                    jackpots.append({
                                        "casino": "City of Dreams Manila",
                                        "game_name": game_name,
                                        "current_amount": amounts[0],
                                        "amount_numeric": re.sub(r'[^\d,.]', '', amounts[0]),
                                        "minimum_bet": amounts[1] if len(amounts) > 1 else "N/A",
                                        "jackpot_type": "Progressive",
                                        "currency": "PHP",
                                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                                        "source_url": url
                                    })
            
            # Method 3: Use JavaScript to extract data directly from the page
            if len(jackpots) == 0:
                logger.info("  Trying JavaScript extraction...")
                try:
                    js_jackpots = page.evaluate(r"""
                        () => {
                            const results = [];
                            
                            // Try to find table rows
                            const rows = document.querySelectorAll('tr, [class*="row"], [class*="item"]');
                            
                            rows.forEach(row => {
                                const text = row.innerText || row.textContent || '';
                                
                                // Look for amounts
                                const amountMatches = text.match(/[P₱]\s*[\d,]+/g);
                                
                                if (amountMatches && amountMatches.length >= 1) {
                                    // Try to extract structured data
                                    const cells = row.querySelectorAll('td, th, div, span');
                                    const cellTexts = Array.from(cells).map(c => c.innerText?.trim()).filter(t => t);
                                    
                                    if (cellTexts.length >= 2) {
                                        results.push({
                                            game: cellTexts[0],
                                            amount: amountMatches[0],
                                            minBet: amountMatches[1] || null,
                                            raw: text
                                        });
                                    }
                                }
                            });
                            
                            return results;
                        }
                    """)
                    
                    logger.info(f"  JavaScript found {len(js_jackpots)} potential entries")
                    
                    for jp in js_jackpots:
                        game_name = jp.get('game', '')
                        amount = jp.get('amount', '')
                        
                        # Filter out headers and invalid entries
                        if game_name and amount and 'GAME' not in game_name.upper() and 'AMOUNT' not in game_name.upper():
                            jackpots.append({
                                "casino": "City of Dreams Manila",
                                "game_name": game_name,
                                "current_amount": amount,
                                "amount_numeric": re.sub(r'[^\d,.]', '', amount),
                                "minimum_bet": jp.get('minBet') or "N/A",
                                "jackpot_type": "Progressive",
                                "currency": "PHP",
                                "scraped_at": datetime.now(timezone.utc).isoformat(),
                                "source_url": url
                            })
                
                except Exception as e:
                    logger.error(f"  JavaScript extraction failed: {e}")
            
            browser.close()
            
            logger.info(f"  Found {len(jackpots)} jackpots")
            
            # Show sample
            if jackpots:
                logger.info("  Sample jackpots:")
                for jp in jackpots[:3]:
                    logger.info(f"    - {jp['game_name']}: {jp['current_amount']}")
            
    except Exception as e:
        logger.error(f"  Error scraping City of Dreams jackpots: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return jackpots


def scrape_solaire_jackpots():
    """Scrape jackpots from Solaire Resort with proper parsing"""
    logger.info("\n=== Scraping Solaire Jackpots ===")
    jackpots = []
    
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not available. Cannot scrape jackpots.")
        return jackpots
    
    url = JACKPOT_CONFIG["solaire"]["url"]
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            logger.info(f"  Loading: {url}")
            page.goto(url, wait_until='domcontentloaded', timeout=60000)
            page.wait_for_selector('.banner-slot', timeout=10000)
            page.wait_for_timeout(5000)
            
            # Extract jackpot data from the page
            jackpot_data = page.evaluate(r"""
                () => {
                    const jackpots = [];
                    
                    // Find all jackpot containers
                    const containers = document.querySelectorAll('.banner-slot');
                    
                    containers.forEach(container => {
                        try {
                            // Extract game name from image src
                            const img = container.querySelector('.jackpot-img');
                            let gameName = 'Unknown Game';
                            
                            if (img && img.src) {
                                // Parse game name from filename
                                const filename = img.src.split('/').pop().replace('.png', '').replace('.webp', '');
                                // Convert underscores and hyphens to spaces, clean up
                                gameName = filename
                                    .replace(/_/g, ' ')
                                    .replace(/-/g, ' ')
                                    .replace(/\d+$/, '') // Remove trailing numbers
                                    .replace(/logo/gi, '')
                                    .replace(/curve/gi, '')
                                    .trim();
                                
                                // Capitalize words
                                gameName = gameName.split(' ')
                                    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
                                    .join(' ');
                                    
                                // Special case mappings
                                const nameMapping = {
                                    'Jinjibaoxi': 'Jin Ji Bao Xi',
                                    'Dl': 'Dragon Link',
                                    'Lightning Link': 'Lightning Link',
                                    'Duo Fu Duo Cai Grand': 'Duo Fu Duo Cai',
                                    'Jotd': 'Jewels of the Dragon',
                                    'Jin Cai Hao Yun': 'Jin Cai Hao Yun',
                                    'Coin Combo': 'Coin Combo',
                                    'Mighty Cash': 'Mighty Cash',
                                    'Lightning Gongs': 'Lightning Gongs',
                                    'Dfdc Thumbnail': 'Duo Fu Duo Cai',
                                    'Dragon Trio': 'Dragon Trio',
                                    'Shfp': 'Super Happy Fortune',
                                    'Bao Zhu Zhao Fu': 'Bao Zhu Zhao Fu',
                                    'Lion Link': 'Lion Link',
                                    'Fu Lai Cai Lai': 'Fu Lai Cai Lai',
                                    'Fafafa': 'FaFaFa',
                                    'Fortune Harmony': 'Fortune Harmony',
                                    'Coin Trio': 'Coin Trio',
                                    'Good Fortune': 'Good Fortune',
                                    'Super Split': 'Super Split'
                                };
                                
                                // Apply mapping if exists
                                for (const [key, value] of Object.entries(nameMapping)) {
                                    if (gameName.toLowerCase().includes(key.toLowerCase())) {
                                        gameName = value;
                                        break;
                                    }
                                }
                            }
                            
                            // Extract jackpot amount
                            const numberContainer = container.querySelector('.banner-slot-number');
                            let amount = '';
                            
                            if (numberContainer) {
                                // Get all digit spans
                                const digits = numberContainer.querySelectorAll('.slot-digit');
                                let amountParts = [];
                                
                                digits.forEach(digit => {
                                    if (digit.classList.contains('seperator')) {
                                        amountParts.push(digit.textContent.trim());
                                    } else {
                                        // Get the first visible number
                                        const spans = digit.querySelectorAll('span');
                                        if (spans.length > 0) {
                                            amountParts.push(spans[0].textContent.trim());
                                        }
                                    }
                                });
                                
                                amount = '₱ ' + amountParts.join('');
                            }
                            
                            if (amount && amount !== '₱ ') {
                                jackpots.push({
                                    game_name: gameName,
                                    amount: amount
                                });
                            }
                        } catch (e) {
                            console.error('Error processing container:', e);
                        }
                    });
                    
                    return jackpots;
                }
            """)
            
            # Process the extracted data
            for jp in jackpot_data:
                amount = jp.get('amount', '')
                if amount and '₱' in amount:
                    # Extract numeric value for sorting/comparison
                    amount_numeric = re.sub(r'[^\d,.]', '', amount)
                    
                    jackpots.append({
                        "casino": "Solaire Resort",
                        "game_name": jp.get('game_name', 'Unknown Game'),
                        "current_amount": amount,
                        "amount_numeric": amount_numeric,
                        "jackpot_type": "Progressive",
                        "currency": "PHP",
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                        "source_url": url
                    })
            
            browser.close()
            logger.info(f"  Successfully scraped {len(jackpots)} jackpots")
            
    except Exception as e:
        logger.error(f"  Error scraping Solaire jackpots: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return jackpots


def save_jackpots(jackpots: List[Dict[str, Any]], folder_name: str):
    """Save jackpots to a dedicated file with better formatting"""
    if not jackpots:
        logger.info("  No jackpots to save")
        return
    
    try:
        jackpot_file = os.path.join(folder_name, "jackpots.json")
        
        # Group by casino
        jackpots_by_casino = {}
        for jp in jackpots:
            casino = jp.get('casino', 'Unknown')
            if casino not in jackpots_by_casino:
                jackpots_by_casino[casino] = []
            jackpots_by_casino[casino].append(jp)
        
        # Calculate statistics
        stats_by_casino = {}
        for casino, jps in jackpots_by_casino.items():
            amounts = []
            for jp in jps:
                try:
                    # Extract numeric value
                    amount_str = jp.get('amount_numeric', '0')
                    amount_num = float(amount_str.replace(',', ''))
                    amounts.append(amount_num)
                except:
                    pass
            
            stats_by_casino[casino] = {
                'count': len(jps),
                'total': sum(amounts) if amounts else 0,
                'average': sum(amounts) / len(amounts) if amounts else 0,
                'highest': max(amounts) if amounts else 0,
                'lowest': min(amounts) if amounts else 0
            }
        
        jackpot_summary = {
            "scraped_at": datetime.now().isoformat(),
            "total_jackpots": len(jackpots),
            "casinos": list(jackpots_by_casino.keys()),
            "statistics_by_casino": stats_by_casino,
            "jackpots_by_casino": jackpots_by_casino,
            "all_jackpots": jackpots
        }
        
        with open(jackpot_file, 'w', encoding='utf-8') as f:
            json.dump(jackpot_summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\nJackpots Summary:")
        logger.info(f"  • Total jackpots found: {len(jackpots)}")
        
        for casino, stats in stats_by_casino.items():
            logger.info(f"\n  {casino}:")
            logger.info(f"    • Count: {stats['count']}")
            logger.info(f"    • Total pool: ₱ {stats['total']:,.2f}")
            logger.info(f"    • Average: ₱ {stats['average']:,.2f}")
            logger.info(f"    • Highest: ₱ {stats['highest']:,.2f}")
            logger.info(f"    • Lowest: ₱ {stats['lowest']:,.2f}")
        
        # Show sample jackpots (highest value ones)
        logger.info(f"\n  Top 5 Jackpots:")
        sorted_jackpots = sorted(
            jackpots, 
            key=lambda x: float(x.get('amount_numeric', '0').replace(',', '') or 0),
            reverse=True
        )
        for i, jp in enumerate(sorted_jackpots[:5], 1):
            logger.info(f"    {i}. {jp.get('casino')} - {jp.get('game_name')}: {jp.get('current_amount')}")
        
        logger.info(f"\n  Saved to: {jackpot_file}")
        
    except Exception as e:
        logger.error(f"  Failed to save jackpots: {e}")
        import traceback
        logger.error(traceback.format_exc())

# -------------------------------
# Main execution
# -------------------------------

def main():
    """Main function to run all scrapers"""
    logger.info("=" * 60)
    logger.info("UNIFIED MANILA CASINO RESORTS SCRAPER")
    logger.info("=" * 60)
    logger.info(f"Output file: {OUTPUT_JSON}")
    logger.info(f"Scraping date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Max pages per casino: {MAX_PAGES_PER_CASINO if MAX_PAGES_PER_CASINO else 'Unlimited'}\n")
    
    all_results = []
    casino_stats = {}
    
    # Run scrapers
    scrapers = [
        ("City of Dreams Manila", scrape_city_of_dreams),
        ("Newport World Resorts", scrape_newport_world),
        ("Okada Manila", scrape_okada),
        ("Solaire Resort", scrape_solaire)
    ]
    
    # Sequential scraping
    for casino_name, scraper_func in scrapers:
        try:
            results = scraper_func()
            all_results.extend(results)
            
            # Collect statistics
            casino_stats[casino_name] = {
                'total_promos': len(results),
                'with_dates': sum(1 for r in results if r.get('temporal_summary', {}).get('has_specific_dates')),
                'with_hours': sum(1 for r in results if r.get('temporal_summary', {}).get('has_operating_hours')),
                'with_validity': sum(1 for r in results if r.get('temporal_summary', {}).get('has_validity_period')),
                'categories': {}
            }
            
            for result in results:
                cat = result.get('category', 'Other')
                if cat not in casino_stats[casino_name]['categories']:
                    casino_stats[casino_name]['categories'][cat] = 0
                casino_stats[casino_name]['categories'][cat] += 1
            
        except Exception as e:
            logger.error(f"Failed to scrape {casino_name}: {e}")
            casino_stats[casino_name] = {'total_promos': 0, 'error': str(e)}
    
    # Print summary statistics
    logger.info(f"\n{'=' * 60}")
    logger.info("SUMMARY STATISTICS")
    logger.info(f"{'=' * 60}")
    logger.info(f"Total promotions scraped: {len(all_results)}")
    logger.info(f"Casinos scraped: {len([c for c in casino_stats if casino_stats[c].get('total_promos', 0) > 0])}/{len(casino_stats)}")
    
    if all_results:
        # organized_folder = create_folder_structure(all_results)
        individual_folder = save_individual_promos(all_results)
        total_with_dates = sum(1 for r in all_results if r.get('temporal_summary', {}).get('has_specific_dates'))
        total_with_hours = sum(1 for r in all_results if r.get('temporal_summary', {}).get('has_operating_hours'))
        total_with_validity = sum(1 for r in all_results if r.get('temporal_summary', {}).get('has_validity_period'))
        
        logger.info(f"\nTemporal Information:")
        logger.info(f"  Promotions with specific dates: {total_with_dates} ({total_with_dates*100//len(all_results)}%)")
        logger.info(f"  Promotions with operating hours: {total_with_hours} ({total_with_hours*100//len(all_results)}%)")
        logger.info(f"  Promotions with validity periods: {total_with_validity} ({total_with_validity*100//len(all_results)}%)")
    
    logger.info(f"\nBreakdown by Casino:")
    for casino, stats in casino_stats.items():
        if 'error' in stats:
            logger.info(f"\n{casino}: ERROR - {stats['error']}")
        else:
            logger.info(f"\n{casino}:")
            logger.info(f"  • Total promotions: {stats['total_promos']}")
            if stats['total_promos'] > 0:
                logger.info(f"  With dates: {stats['with_dates']} ({stats['with_dates']*100//stats['total_promos']}%)")
                logger.info(f"  With hours: {stats['with_hours']} ({stats['with_hours']*100//stats['total_promos']}%)")
                logger.info(f"  With validity: {stats['with_validity']} ({stats['with_validity']*100//stats['total_promos']}%)")
                logger.info(f"  Categories: {', '.join(f'{cat} ({count})' for cat, count in stats['categories'].items())}")
    
    # Category breakdown
    all_categories = {}
    for result in all_results:
        cat = result.get('category', 'Other')
        if cat not in all_categories:
            all_categories[cat] = 0
        all_categories[cat] += 1
    
    if all_categories:
        logger.info(f"\nCategory Distribution (All Casinos):")
        for cat, count in sorted(all_categories.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {cat}: {count} promotions")
    
    logger.info(f"\n{'=' * 60}")
    logger.info("SCRAPING COMPLETE!")
    logger.info(f"{'=' * 60}")

     # ========== SCRAPE JACKPOTS ==========
    logger.info(f"\n{'=' * 60}")
    logger.info("SCRAPING JACKPOTS")
    logger.info(f"{'=' * 60}")
    
    all_jackpots = []
    
    # Scrape City of Dreams jackpots
    try:
        cod_jackpots = scrape_city_of_dreams_jackpots()
        all_jackpots.extend(cod_jackpots)
    except Exception as e:
        logger.error(f"Failed to scrape City of Dreams jackpots: {e}")
    
    # Scrape Solaire jackpots
    try:
        solaire_jackpots = scrape_solaire_jackpots()
        all_jackpots.extend(solaire_jackpots)
    except Exception as e:
        logger.error(f"Failed to scrape Solaire jackpots: {e}")
    
    # Save jackpots to the promo folder
    if individual_folder:
        save_jackpots(all_jackpots, individual_folder)
    
    return all_results

# Add this helper function for filename sanitization
def sanitize_filename(filename: str, max_length: int = 100) -> str:
    """
    Sanitize filename by removing invalid characters and limiting length
    """
    # Remove leading/trailing whitespace
    filename = filename.strip()
    
    # Replace invalid characters with underscore
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove control characters
    filename = ''.join(char for char in filename if unicodedata.category(char)[0] != 'C')
    
    # Replace multiple spaces/underscores with single underscore
    filename = re.sub(r'[_\s]+', '_', filename)
    
    # Remove leading/trailing underscores and dots
    filename = filename.strip('_.')
    
    # Limit length
    if len(filename) > max_length:
        filename = filename[:max_length].rsplit('_', 1)[0]  # Cut at last underscore before limit
    
    return filename if filename else "unnamed"


def save_individual_promos(all_results: List[Dict[str, Any]], base_filename: str = OUTPUT_JSON):
    """
    Save each promo as an individual JSON file in a timestamped folder
    
    Args:
        all_results: List of all scraped promotions
        base_filename: Base filename to extract timestamp from
    
    Returns:
        str: Path to the created folder
    """
    if not all_results:
        logger.warning("No results to save individually")
        return None
    
    # Create folder name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"/app/output/promo_{timestamp}" 
    
    # Create the folder
    os.makedirs(folder_name, exist_ok=True)
    logger.info(f"\n=== SAVING INDIVIDUAL PROMO FILES ===")
    logger.info(f"Creating folder: {folder_name}")
    
    # Statistics
    saved_count = 0
    failed_count = 0
    casino_counts = {}
    
    for i, promo in enumerate(all_results, 1):
        try:
            # Get promo details
            casino = promo.get('casino', 'Unknown')
            title = promo.get('title', 'Untitled')
            promo_id = promo.get('id', hashlib.sha256(str(i).encode()).hexdigest()[:8])
            
            # Track casino counts
            if casino not in casino_counts:
                casino_counts[casino] = 0
            casino_counts[casino] += 1
            
            # Create filename
            # Format: {casino_slug}_{sanitized_title}_{id}.json
            casino_slug = sanitize_filename(casino.lower().replace(' ', '_'), max_length=30)
            title_slug = sanitize_filename(title.lower().replace(' ', '_'), max_length=50)
            
            filename = f"{i}_{title_slug}_{promo_id[:8]}.json"
            filepath = os.path.join(folder_name, filename)
            
            # Save individual JSON
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(promo, f, indent=2, ensure_ascii=False)
            
            saved_count += 1
            
            if i <= 5:  # Show first 5 for verification
                logger.info(f"  Saved: {filename}")
            elif i == 6:
                logger.info(f"  ... saving remaining files ...")
            
        except Exception as e:
            logger.error(f"  Failed to save promo {i}: {e}")
            failed_count += 1
    
    # Summary
    logger.info(f"\nIndividual Files Summary:")
    logger.info(f"  Total files saved: {saved_count}")
    logger.info(f"  Failed: {failed_count}")
    logger.info(f"  Location: {os.path.abspath(folder_name)}")
    
    logger.info(f"\n  Breakdown by Casino:")
    for casino, count in sorted(casino_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"    {casino}: {count} files")
    
    # Create a summary index file
    try:
        summary_file = os.path.join(folder_name, "_index.json")
        summary_data = {
            "generated_at": datetime.now().isoformat(),
            "total_promotions": len(all_results),
            "casinos": list(casino_counts.keys()),
            "casino_counts": casino_counts,
            "files": [
                {
                    "filename": f"{sanitize_filename(p.get('casino', 'unknown').lower().replace(' ', '_'), 30)}_{sanitize_filename(p.get('title', 'untitled').lower().replace(' ', '_'), 50)}_{p.get('id', '')[:8]}.json",
                    "casino": p.get('casino'),
                    "title": p.get('title'),
                    "category": p.get('category'),
                    "url": p.get('url')
                }
                for p in all_results
            ]
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\nIndex file created: _index.json")
        
    except Exception as e:
        logger.error(f"  Failed to create index file: {e}")
    
    return folder_name


def create_folder_structure(all_results: List[Dict[str, Any]]):
    """
    Create organized folder structure with promos grouped by casino
    
    Args:
        all_results: List of all scraped promotions
    
    Returns:
        str: Path to the created main folder
    """
    if not all_results:
        logger.warning("No results to organize")
        return None
    
    # Create main folder with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    main_folder = f"promo_{timestamp}"
    
    logger.info(f"\n=== CREATING ORGANIZED FOLDER STRUCTURE ===")
    logger.info(f"Main folder: {main_folder}")
    
    # Group results by casino
    casino_groups = {}
    for promo in all_results:
        casino = promo.get('casino', 'Unknown')
        if casino not in casino_groups:
            casino_groups[casino] = []
        casino_groups[casino].append(promo)
    
    # Create subfolders for each casino
    total_saved = 0
    for casino, promos in casino_groups.items():
        # Create casino subfolder
        casino_slug = sanitize_filename(casino.lower().replace(' ', '_'), max_length=50)
        casino_folder = os.path.join(main_folder, casino_slug)
        os.makedirs(casino_folder, exist_ok=True)
        
        logger.info(f"\n{casino} ({len(promos)} promos)")
        
        # Save each promo in casino folder
        for i, promo in enumerate(promos, 1):
            try:
                title = promo.get('title', 'Untitled')
                promo_id = promo.get('id', '')[:8]
                
                title_slug = sanitize_filename(title.lower().replace(' ', '_'), max_length=60)
                filename = f"{i:03d}_{title_slug}_{promo_id}.json"
                filepath = os.path.join(casino_folder, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(promo, f, indent=2, ensure_ascii=False)
                
                total_saved += 1
                
                if i <= 3:
                    logger.info(f"    {filename}")
                elif i == 4 and len(promos) > 4:
                    logger.info(f"    ... {len(promos) - 3} more files ...")
                
            except Exception as e:
                logger.error(f"    Failed to save: {e}")
    
    # Create main index
    try:
        index_file = os.path.join(main_folder, "_index.json")
        index_data = {
            "generated_at": datetime.now().isoformat(),
            "total_promotions": len(all_results),
            "casinos": {
                casino: {
                    "count": len(promos),
                    "folder": sanitize_filename(casino.lower().replace(' ', '_'), 50)
                }
                for casino, promos in casino_groups.items()
            }
        }
        
        with open(index_file, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\nTotal files saved: {total_saved}")
        logger.info(f"Index file: _index.json")
        logger.info(f"Location: {os.path.abspath(main_folder)}")
        
    except Exception as e:
        logger.error(f"  Failed to create index: {e}")
    
    return main_folder


if __name__ == "__main__":

    results = main()


