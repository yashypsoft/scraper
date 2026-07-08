import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapy.http import HtmlResponse
from scripts.run_ashley_scraper import AshleyURLSpider

def test_html_parsing_fallback():
    # Setup dummy spider
    spider = AshleyURLSpider(
        base_api="https://colemanfurniture.com/catalog/category/view/id/10611?manufacturer=moes+home",
        start_page="1",
        end_page="1"
    )
    spider.url_list = []
    
    # HTML content snippet containing product-img-frame
    html_content = """
    <html>
        <body>
            <div class="product-grid">
                <div class="product-grid-items">
                    <div class="product-grid-item" data-position="position-1" data-llm-sku="sku--MOE-OA-1110-37">
                        <div class="product-img-frame">
                            <a href="https://colemanfurniture.com/betty-beige-stripe-20-cushion.htm" aria-label="Betty Beige Stripe 20&quot; Cushion">
                                <img src="thumb.jpg" />
                            </a>
                        </div>
                    </div>
                    <div class="product-grid-item" data-position="position-2" data-llm-sku="sku--MOE-OA-1110-38">
                        <div class="product-img-frame">
                            <a href="/another-product.htm" aria-label="Another Product">
                                <img src="thumb2.jpg" />
                            </a>
                        </div>
                    </div>
                </div>
            </div>
        </body>
    </html>
    """
    
    from scrapy import Request
    request = Request(
        url="https://colemanfurniture.com/catalog/category/view/id/10611?manufacturer=moes+home&p=1",
        meta={'page': 1}
    )
    response = HtmlResponse(
        url="https://colemanfurniture.com/catalog/category/view/id/10611?manufacturer=moes+home&p=1",
        body=html_content.encode('utf-8'),
        encoding='utf-8',
        request=request
    )
    
    spider.parse_page(response)
    
    # Assertions
    expected_urls = [
        "https://colemanfurniture.com/betty-beige-stripe-20-cushion.htm",
        "https://colemanfurniture.com/another-product.htm"
    ]
    assert len(spider.url_list) == 2, f"Expected 2 URLs, got {len(spider.url_list)}"
    assert spider.url_list[0] == expected_urls[0], f"Expected {expected_urls[0]}, got {spider.url_list[0]}"
    assert spider.url_list[1] == expected_urls[1], f"Expected {expected_urls[1]}, got {spider.url_list[1]}"
    print("HTML Fallback Test Passed Successfully!")

def test_json_scanning_fallback():
    # Setup dummy spider
    spider = AshleyURLSpider(
        base_api="https://colemanfurniture.com/living/accent-pillows.htm?manufacturer=moes+home",
        start_page="1",
        end_page="1"
    )
    spider.url_list = []
    
    # JSON payload mimicking category response with some product URLs and listing canonical
    json_payload = """
    {
        "data": {
            "head": {
                "meta": {
                    "canonical": "https://colemanfurniture.com/living/accent-pillows.htm"
                }
            },
            "meta": {
                "uri": "/living/accent-pillows.htm",
                "pageType": "listing"
            },
            "products_nested": [
                {
                    "sku": "MOE-OA-1110-37",
                    "url": "https://colemanfurniture.com/betty-beige-stripe-20-cushion.htm"
                },
                {
                    "sku": "MOE-OA-1110-38",
                    "url": "/another-product.htm"
                }
            ],
            "random_string_urls": [
                "https://colemanfurniture.com/sub-cat/ignored.htm",
                "/yet-another-valid-product.htm"
            ]
        }
    }
    """
    
    from scrapy import Request
    from scrapy.http import TextResponse
    request = Request(
        url="https://colemanfurniture.com/living/accent-pillows.htm?manufacturer=moes+home&p=1",
        meta={'page': 1}
    )
    response = TextResponse(
        url="https://colemanfurniture.com/living/accent-pillows.htm?manufacturer=moes+home&p=1",
        headers={'Content-Type': 'application/json'},
        body=json_payload.encode('utf-8'),
        encoding='utf-8',
        request=request
    )
    
    spider.parse_page(response)
    
    expected_urls = [
        "https://colemanfurniture.com/betty-beige-stripe-20-cushion.htm",
        "https://colemanfurniture.com/another-product.htm",
        "https://colemanfurniture.com/yet-another-valid-product.htm"
    ]
    # Note: "/sub-cat/ignored.htm" has a slash in its path, so it should be ignored.
    # The canonical "https://colemanfurniture.com/living/accent-pillows.htm" also has a slash in its path, so it should be ignored.
    
    assert len(spider.url_list) == 3, f"Expected 3 URLs, got {len(spider.url_list)}: {spider.url_list}"
    for exp_url in expected_urls:
        assert exp_url in spider.url_list, f"Expected {exp_url} to be in {spider.url_list}"
        
    print("JSON Scanning Fallback Test Passed Successfully!")

if __name__ == '__main__':
    test_html_parsing_fallback()
    test_json_scanning_fallback()
