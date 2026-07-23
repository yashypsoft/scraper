-- =============================================================================
-- OSB Scraping Schema — Optimized
-- • TEXT only where data is truly unbounded (raw_html, long descriptions, URLs)
-- • VARCHAR(n) for all short/bounded strings
-- • BIGINT for GTINs (14-digit numerics)
-- • BOOLEAN for true/false fields
-- • NUMERIC(8,2) for physical dimensions
-- • TIMESTAMPTZ throughout (timezone-aware)
-- =============================================================================

CREATE TABLE osb_products (
    product_id      INTEGER         PRIMARY KEY,            -- Magento entity_id, always numeric
    web_id          VARCHAR(32),                            -- UPC / internal barcode (bounded)

    sku             VARCHAR(128),
    mpn             VARCHAR(128),
    gtin            BIGINT,                                 -- 13–14 digit EAN/UPC
    part_number     VARCHAR(512),

    name            VARCHAR(512)    NOT NULL DEFAULT '',
    brand           VARCHAR(128),
    collection      VARCHAR(128),
    product_type    VARCHAR(128),

    color           VARCHAR(64),
    size            VARCHAR(64),

    price           NUMERIC(12,2),
    map_price       NUMERIC(12,2),
    margin          NUMERIC(8,4),                          -- percentage, 4dp is enough

    osb_url         VARCHAR(1024),

    grouping_attr_1         VARCHAR(128),
    grouping_attr_1_value   VARCHAR(128),
    grouping_attr_2         VARCHAR(128),
    grouping_attr_2_value   VARCHAR(128),

    -- product-variant option attributes (all bounded enum values)
    bed_size_measure    VARCHAR(64),
    fireplace_option    VARCHAR(64),
    layout_icon         VARCHAR(64),
    rug_size            VARCHAR(64),
    mattress_size       VARCHAR(64),
    power_option        VARCHAR(64),
    dimension_text      VARCHAR(256),
    comfort_level       VARCHAR(64),
    mattress_thickness  VARCHAR(64),

    mfr_sales_30d   INTEGER         DEFAULT 0,
    status          SMALLINT        DEFAULT 1,

    scraping_status VARCHAR(20)     DEFAULT 'pending',      -- pending | running | completed | failed

    claimed_by      VARCHAR(128),
    claimed_at      TIMESTAMPTZ,

    keyword         VARCHAR(2048),                          -- constructed Google search URL
    url             VARCHAR(2048),                          -- full Google Shopping search URL
    last_attempt    TIMESTAMPTZ,
    error_message   VARCHAR(1024),

    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX idx_osb_scrape_queue
    ON osb_products (status, scraping_status, mfr_sales_30d DESC);

CREATE INDEX idx_osb_brand   ON osb_products (brand);
CREATE INDEX idx_osb_gtin    ON osb_products (gtin);
CREATE INDEX idx_osb_mpn     ON osb_products (mpn);


-- =============================================================================

CREATE TABLE competitors (
    competitor_id   SERIAL          PRIMARY KEY,
    competitor_name VARCHAR(128)    UNIQUE NOT NULL,
    base_url        VARCHAR(512),
    status          SMALLINT        DEFAULT 1,
    supports_scraping BOOLEAN       DEFAULT TRUE,
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);


-- =============================================================================

CREATE TABLE google_shopping_results (
    id              SERIAL          PRIMARY KEY,
    product_id      INTEGER         NOT NULL
                        REFERENCES osb_products (product_id)
                        ON DELETE CASCADE,
    card_index      SMALLINT        DEFAULT 1,

    google_title        VARCHAR(512),
    google_description  TEXT,                               -- can be long; keep TEXT

    gs_main_image   VARCHAR(1024),
    gs_images       JSONB,

    brand           VARCHAR(128),
    color           TEXT,                                   -- can be very long (3700+ chars observed)

    -- physical dimensions stored as numbers for future math/filtering
    width           NUMERIC(8,2),
    height          NUMERIC(8,2),
    depth           NUMERIC(8,2),

    style           VARCHAR(256),
    material        VARCHAR(256),
    shape           VARCHAR(128),

    assembly_required   BOOLEAN,

    weight          NUMERIC(8,2),                           -- kg or lbs, scraper normalises

    rating_star     NUMERIC(3,2),
    rating_count    INTEGER,

    typical_price_low   NUMERIC(12,2),
    typical_price_high  NUMERIC(12,2),

    best_price_url          VARCHAR(1024),
    popular_url             VARCHAR(1024),

    other_attributes    JSONB,

    last_response           TEXT,                           -- raw API/HTML response; truly unbounded
    osb_url_match           VARCHAR(1024),
    google_seller_page_url  VARCHAR(2048),
    cid                     VARCHAR(64),
    pid                     VARCHAR(64),
    osb_position            SMALLINT        DEFAULT 0,
    osb_id                  VARCHAR(256),
    seller_count            SMALLINT        DEFAULT 0,
    status                  VARCHAR(32),

    scraped_at  TIMESTAMPTZ     DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     DEFAULT NOW(),

    CONSTRAINT uk_product_card UNIQUE (product_id, card_index)
);

CREATE INDEX idx_gsr_product ON google_shopping_results (product_id);


-- =============================================================================
-- NOTE: No unique constraint on (product_id, competitor_id) —
-- a competitor can have multiple listings for the same product (different variants, prices, etc.)

CREATE TABLE google_shopping_sellers (
    seller_listing_id   BIGSERIAL   PRIMARY KEY,

    product_id      INTEGER
                        REFERENCES osb_products (product_id)
                        ON DELETE CASCADE,

    competitor_id   INTEGER
                        REFERENCES competitors (competitor_id),

    seller_name         VARCHAR(256),
    seller_product_name VARCHAR(512),
    seller_url          VARCHAR(2048),

    price               NUMERIC(12,2),
    original_price      NUMERIC(12,2),
    discount_amount     NUMERIC(10,2),

    coupon_code         VARCHAR(64),
    coupon_remark       VARCHAR(256),

    stock_status        VARCHAR(32),

    seller_rating       NUMERIC(3,2),

    delivery_tagline    VARCHAR(256),

    google_position     SMALLINT,

    site_display        VARCHAR(256),
    is_me               BOOLEAN     DEFAULT FALSE,

    scraped_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_gss_product   ON google_shopping_sellers (product_id);
CREATE INDEX idx_gss_competitor ON google_shopping_sellers (competitor_id);


-- =============================================================================
-- TODO: Once URLs are scraped from Google Shopping, new URLs from sitemaps will
-- be added here for direct seller scraping.

CREATE TABLE seller_scrape_jobs (
    scraping_id     BIGSERIAL   PRIMARY KEY,

    product_id      INTEGER
                        REFERENCES osb_products (product_id)
                        ON DELETE CASCADE,

    seller_listing_id   BIGINT
                        REFERENCES google_shopping_sellers (seller_listing_id)
                        ON DELETE SET NULL,

    competitor_id   INTEGER
                        REFERENCES competitors (competitor_id),

    scraping_url    VARCHAR(2048)   NOT NULL,

    scraping_status VARCHAR(20)     DEFAULT 'pending',      -- pending | running | completed | failed

    priority        SMALLINT        DEFAULT 100,
    retry_count     SMALLINT        DEFAULT 0,

    claimed_by      VARCHAR(128),
    claimed_at      TIMESTAMPTZ,

    last_error      VARCHAR(1024),

    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_ssj_queue
    ON seller_scrape_jobs (scraping_status, priority DESC, created_at);

CREATE INDEX idx_ssj_product ON seller_scrape_jobs (product_id);


-- =============================================================================

CREATE TABLE seller_product_details (
    seller_product_detail_id    BIGSERIAL   PRIMARY KEY,

    scraping_id     BIGINT
                        REFERENCES seller_scrape_jobs (scraping_id)
                        ON DELETE CASCADE,

    product_id      INTEGER
                        REFERENCES osb_products (product_id)
                        ON DELETE CASCADE,

    competitor_id   INTEGER
                        REFERENCES competitors (competitor_id),

    seller_product_id   VARCHAR(128),
    seller_variant_id   VARCHAR(128),

    seller_category     VARCHAR(256),
    seller_category_url VARCHAR(1024),

    seller_brand        VARCHAR(128),
    seller_product_name VARCHAR(512),

    seller_sku      VARCHAR(64),
    seller_mpn      VARCHAR(64),
    seller_gtin     BIGINT,                                 -- 13–14 digit EAN/UPC

    seller_price    NUMERIC(12,2),
    seller_qty      INTEGER,

    seller_color    VARCHAR(64),

    -- physical dimensions as numbers
    seller_weight   NUMERIC(8,2),
    seller_height   NUMERIC(8,2),
    seller_width    NUMERIC(8,2),
    seller_depth    NUMERIC(8,2),

    seller_dimension    VARCHAR(256),                       -- raw text if not parseable

    seller_product_status   VARCHAR(32),
    seller_highlights       TEXT,                           -- bullet list; can be long

    seller_main_image       VARCHAR(1024),
    seller_other_images     JSONB,

    extra_details   JSONB,
    raw_html        TEXT,                                   -- full page HTML; truly unbounded

    scraped_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_spd_product    ON seller_product_details (product_id);
CREATE INDEX idx_spd_scraping   ON seller_product_details (scraping_id);