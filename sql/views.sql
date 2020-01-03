IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE  table_name = 'year_usage_stats') THEN

CREATE MATERIALIZED VIEW year_usage_stats AS
with "download_stats" AS (SELECT ul.id AS ulid, pi_access, staff_access, COUNT(1) AS "count", SUM(diskfile_file_size) AS bytes,
SUM(released::int) AS released,
SUM(released::int * diskfile_file_size) AS released_bytes
FROM filedownloadlog AS fdl JOIN usagelog AS ul ON fdl.usagelog_id = ul.id
GROUP BY ul.id, fdl.pi_access, fdl.staff_access),
"upload_stats" AS (
SELECT ul.id AS ulid, COUNT(1) as "count", SUM(ful.size) AS bytes
FROM fileuploadlog AS ful JOIN usagelog AS ul ON ful.usagelog_id = ul.id
GROUP BY ulid
)
SELECT EXTRACT(YEAR FROM utdatetime) as "yr",
SUM(CAST ((ul.status = 200) AS INTEGER)) as hits_ok,
SUM(CAST ((ul.status >= 400) AS INTEGER)) as hits_fail,
SUM(CAST ((ul.status = 200 AND ul.this = 'searchform') AS INTEGER)) as search_ok,
SUM(CAST ((ul.status >= 400 AND ul.this = 'searchform') AS INTEGER)) as search_fail,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * ds.count) as downloads_total,
SUM(ds.bytes) as bytes_total,
SUM(CAST (ul.status = 200 AND us.ulid IS NOT NULL AS INTEGER)) as uploads_total,
SUM(us.bytes) as ul_bytes_total,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.pi_access AS INTEGER) * ds.count) as pi_downloads,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.pi_access AS INTEGER) * ds.bytes) as pi_dl_bytes,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.staff_access AS INTEGER) * ds.count) as staff_downloads,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.staff_access AS INTEGER) * ds.bytes) as staff_dl_bytes,
SUM(ds.released) as public_downloads,
SUM(ds.released_bytes) as public_dl_bytes,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL AND ul.user_id IS NULL) AS INTEGER) * ds.count) as anon_downloads,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL AND ul.user_id IS NULL) AS INTEGER) * ds.bytes) as anon_dl_bytes,
SUM(CAST((ul.status >= 400 AND ds.ulid IS NOT NULL) AS INTEGER)) as failed
FROM usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid
LEFT JOIN upload_stats AS us ON ul.id = us.ulid
GROUP BY yr -- EXTRACT(....)
ORDER BY yr -- Using the column position to avoid repeating the whole
;

---------------SELECT CURRENT_DATE + CAST(-extract(dow FROM CURRENT_DATE) AS INT) % 7 + INTERVAL '1 week' AS most_recent_sunday;

CREATE MATERIALIZED VIEW week_usage_stats AS
with "download_stats" AS (SELECT ul.id AS ulid, pi_access, staff_access, COUNT(1) AS "count", SUM(diskfile_file_size) AS bytes,
SUM(released::int) AS released,
SUM(released::int * diskfile_file_size) AS released_bytes
FROM filedownloadlog AS fdl JOIN usagelog AS ul ON fdl.usagelog_id = ul.id
GROUP BY ul.id, fdl.pi_access, fdl.staff_access),
"upload_stats" AS (
SELECT ul.id AS ulid, COUNT(1) as "count", SUM(ful.size) AS bytes
FROM fileuploadlog AS ful JOIN usagelog AS ul ON ful.usagelog_id = ul.id
GROUP BY ulid
),
"strt" AS (
    SELECT generate_series(DATE(NOW()) - INTERVAL '12 month', CURRENT_DATE + CAST(-extract(dow FROM CURRENT_DATE) AS INT) % 7 + INTERVAL '1 week', INTERVAL '1 week')
)
SELECT ts.generate_series as "tme",
SUM(CAST ((ul.status = 200) AS INTEGER)) as hits_ok,
SUM(CAST ((ul.status >= 400) AS INTEGER)) as hits_fail,
SUM(CAST ((ul.status = 200 AND ul.this = 'searchform') AS INTEGER)) as search_ok,
SUM(CAST ((ul.status >= 400 AND ul.this = 'searchform') AS INTEGER)) as search_fail,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * ds.count) as downloads_total,
SUM(ds.bytes) as bytes_total,
SUM(CAST (ul.status = 200 AND us.ulid IS NOT NULL AS INTEGER)) as uploads_total,
SUM(us.bytes) as ul_bytes_total,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.pi_access AS INTEGER) * ds.count) as pi_downloads,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.pi_access AS INTEGER) * ds.bytes) as pi_dl_bytes,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.staff_access AS INTEGER) * ds.count) as staff_downloads,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.staff_access AS INTEGER) * ds.bytes) as staff_dl_bytes,
SUM(ds.released) as public_downloads,
SUM(ds.released_bytes) as public_dl_bytes,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL AND ul.user_id IS NULL) AS INTEGER) * ds.count) as anon_downloads,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL AND ul.user_id IS NULL) AS INTEGER) * ds.bytes) as anon_dl_bytes,
SUM(CAST((ul.status >= 400 AND ds.ulid IS NOT NULL) AS INTEGER)) as failed
FROM usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid
LEFT JOIN upload_stats AS us ON ul.id = us.ulid
INNER JOIN strt AS ts ON ul.utdatetime BETWEEN ts.generate_series AND ts.generate_series + INTERVAL '1 week' - INTERVAL '1 microsecond'
GROUP BY ts.generate_series
ORDER BY ts.generate_series
;

-----------------------

CREATE MATERIALIZED VIEW day_usage_stats AS
with "download_stats" AS (SELECT ul.id AS ulid, pi_access, staff_access, COUNT(1) AS "count", SUM(diskfile_file_size) AS bytes,
SUM(released::int) AS released,
SUM(released::int * diskfile_file_size) AS released_bytes
FROM filedownloadlog AS fdl JOIN usagelog AS ul ON fdl.usagelog_id = ul.id
GROUP BY ul.id, fdl.pi_access, fdl.staff_access),
"upload_stats" AS (
SELECT ul.id AS ulid, COUNT(1) as "count", SUM(ful.size) AS bytes
FROM fileuploadlog AS ful JOIN usagelog AS ul ON ful.usagelog_id = ul.id
GROUP BY ulid
),
"strt" AS (
    SELECT generate_series(DATE(NOW()) - INTERVAL '12 month', DATE(NOW()), INTERVAL '1 day')
)
SELECT ts.generate_series as "tme",
SUM(CAST ((ul.status = 200) AS INTEGER)) as hits_ok,
SUM(CAST ((ul.status >= 400) AS INTEGER)) as hits_fail,
SUM(CAST ((ul.status = 200 AND ul.this = 'searchform') AS INTEGER)) as search_ok,
SUM(CAST ((ul.status >= 400 AND ul.this = 'searchform') AS INTEGER)) as search_fail,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * ds.count) as downloads_total,
SUM(ds.bytes) as bytes_total,
SUM(CAST (ul.status = 200 AND us.ulid IS NOT NULL AS INTEGER)) as uploads_total,
SUM(us.bytes) as ul_bytes_total,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.pi_access AS INTEGER) * ds.count) as pi_downloads,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.pi_access AS INTEGER) * ds.bytes) as pi_dl_bytes,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.staff_access AS INTEGER) * ds.count) as staff_downloads,
SUM(CAST((ul.status = 200 AND ds.ulid IS NOT NULL) AS INTEGER) * CAST(ds.staff_access AS INTEGER) * ds.bytes) as staff_dl_bytes,
SUM(ds.released) as public_downloads,
SUM(ds.released_bytes) as public_dl_bytes,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL AND ul.user_id IS NULL) AS INTEGER) * ds.count) as anon_downloads,
SUM(CAST ((ul.status = 200 AND ds.ulid IS NOT NULL AND ul.user_id IS NULL) AS INTEGER) * ds.bytes) as anon_dl_bytes,
SUM(CAST((ul.status >= 400 AND ds.ulid IS NOT NULL) AS INTEGER)) as failed
FROM usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid
LEFT JOIN upload_stats AS us ON ul.id = us.ulid
INNER JOIN strt AS ts ON ul.utdatetime BETWEEN ts.generate_series AND ts.generate_series + INTERVAL '1 day' - INTERVAL '1 microsecond'
GROUP BY ts.generate_series
ORDER BY ts.generate_series


END IF
