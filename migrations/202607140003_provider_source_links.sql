ALTER TABLE earthquake_source_records
    ADD COLUMN source_url TEXT,
    ADD COLUMN detail_url TEXT;

UPDATE earthquake_source_records AS source
SET source_url = earthquake.source_url,
    detail_url = earthquake.detail_url
FROM earthquakes AS earthquake
WHERE source.earthquake_id = earthquake.id
  AND source.provider = earthquake.preferred_source
  AND source.external_id = earthquake.preferred_external_id;

UPDATE earthquake_source_records
SET source_url = COALESCE(source_url, raw_payload -> 'properties' ->> 'url'),
    detail_url = COALESCE(detail_url, raw_payload -> 'properties' ->> 'detail')
WHERE provider = 'usgs';
