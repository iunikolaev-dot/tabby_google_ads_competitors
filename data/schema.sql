-- data/schema.sql — SoT schema for ads.db
--
-- One row per (platform, creative_id, region) — matches the merge key used
-- by run_weekly.py:merge_and_generate. The same Creative ID observed in two
-- regions stays as two rows here (the existing JSON has 178 such pairs).
-- Dedupe is a future concern (would need dashboard region-filter changes).

CREATE TABLE IF NOT EXISTS ads (
    platform                TEXT NOT NULL,
    creative_id             TEXT NOT NULL,
    region                  TEXT NOT NULL DEFAULT '',

    -- Identity / categorization
    competitor_name         TEXT NOT NULL DEFAULT '',
    competitor_website      TEXT DEFAULT '',
    category                TEXT DEFAULT '',
    advertiser_id           TEXT DEFAULT '',
    advertiser_name         TEXT DEFAULT '',
    regions_csv             TEXT DEFAULT '',

    -- Creative
    ad_format               TEXT DEFAULT '',
    image_url               TEXT DEFAULT '',
    video_url               TEXT DEFAULT '',
    embed_url               TEXT DEFAULT '',
    ad_preview_url          TEXT DEFAULT '',
    landing_page            TEXT DEFAULT '',
    screenshot              TEXT DEFAULT '',

    -- Lifecycle
    status                  TEXT DEFAULT 'Active',
    started_running         TEXT DEFAULT '',
    last_shown              TEXT DEFAULT '',
    date_collected          TEXT DEFAULT '',
    new_this_week           TEXT DEFAULT '',

    -- History tracking (audit step 2)
    seen_in_batches         INTEGER DEFAULT 1,
    first_seen_batch_id     TEXT DEFAULT '',
    last_seen_batch_id      TEXT DEFAULT '',
    miss_streak             INTEGER DEFAULT 0,
    scrape_batch_id         TEXT DEFAULT '',

    -- v2 metadata
    schema_version          INTEGER DEFAULT 2,
    source_actor            TEXT DEFAULT '',
    preview_status          TEXT DEFAULT 'unverified',
    preview_checked_at      TEXT DEFAULT '',
    retired                 INTEGER DEFAULT 0,
    retired_reason          TEXT DEFAULT '',

    -- Composite key (table-level constraint MUST come after all columns)
    PRIMARY KEY (platform, creative_id, region)
);

CREATE INDEX IF NOT EXISTS idx_ads_status         ON ads(status);
CREATE INDEX IF NOT EXISTS idx_ads_competitor     ON ads(competitor_name);
CREATE INDEX IF NOT EXISTS idx_ads_platform       ON ads(platform);
CREATE INDEX IF NOT EXISTS idx_ads_format         ON ads(ad_format);
CREATE INDEX IF NOT EXISTS idx_ads_last_shown     ON ads(last_shown);
CREATE INDEX IF NOT EXISTS idx_ads_seen_batches   ON ads(seen_in_batches);
CREATE INDEX IF NOT EXISTS idx_ads_category       ON ads(category);
