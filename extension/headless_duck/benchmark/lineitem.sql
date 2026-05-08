-- Headless Duck TPC-H lineitem benchmark harness.
--
-- Run from the repository root after creating the output directory:
--   rm -rf /tmp/headless_duck_lineitem_benchmark
--   mkdir -p /tmp/headless_duck_lineitem_benchmark
--   build/release/duckdb < extension/headless_duck/benchmark/lineitem.sql

LOAD tpch;
LOAD parquet;
LOAD headless_duck;

SET threads=8;
SET preserve_insertion_order=false;

CREATE OR REPLACE TABLE hduck_lineitem_benchmark_events (
	label VARCHAR,
	phase VARCHAR,
	at_ms BIGINT
);

CREATE OR REPLACE TABLE hduck_lineitem_benchmark_results (
	query_name VARCHAR,
	source_format VARCHAR,
	row_count BIGINT,
	revenue DECIMAL(38, 4),
	quantity_sum DECIMAL(38, 4),
	min_shipdate DATE,
	max_shipdate DATE
);

-- Generate source data. Change sf=1 to a smaller value for quick smoke runs.
INSERT INTO hduck_lineitem_benchmark_events VALUES ('dbgen', 'start', epoch_ms(current_timestamp));
CALL dbgen(sf=1);
INSERT INTO hduck_lineitem_benchmark_events VALUES ('dbgen', 'end', epoch_ms(current_timestamp));

-- Write standalone files.
INSERT INTO hduck_lineitem_benchmark_events VALUES ('write_hduck', 'start', epoch_ms(current_timestamp));
COPY lineitem TO '/tmp/headless_duck_lineitem_benchmark/lineitem.hduck' (FORMAT headless_duck);
INSERT INTO hduck_lineitem_benchmark_events VALUES ('write_hduck', 'end', epoch_ms(current_timestamp));

INSERT INTO hduck_lineitem_benchmark_events VALUES ('write_parquet', 'start', epoch_ms(current_timestamp));
COPY lineitem TO '/tmp/headless_duck_lineitem_benchmark/lineitem.parquet' (FORMAT parquet);
INSERT INTO hduck_lineitem_benchmark_events VALUES ('write_parquet', 'end', epoch_ms(current_timestamp));

INSERT INTO hduck_lineitem_benchmark_events VALUES ('write_duckdb', 'start', epoch_ms(current_timestamp));
ATTACH '/tmp/headless_duck_lineitem_benchmark/lineitem.duckdb' AS lineitem_file;
CREATE TABLE lineitem_file.lineitem AS SELECT * FROM lineitem;
CHECKPOINT lineitem_file;
DETACH lineitem_file;
INSERT INTO hduck_lineitem_benchmark_events VALUES ('write_duckdb', 'end', epoch_ms(current_timestamp));

CREATE OR REPLACE TABLE hduck_lineitem_benchmark_file_sizes AS
SELECT 'hduck' AS source_format, size AS size_bytes
FROM read_blob('/tmp/headless_duck_lineitem_benchmark/lineitem.hduck')
UNION ALL
SELECT 'parquet' AS source_format, size AS size_bytes
FROM read_blob('/tmp/headless_duck_lineitem_benchmark/lineitem.parquet')
UNION ALL
SELECT 'duckdb' AS source_format, size AS size_bytes
FROM read_blob('/tmp/headless_duck_lineitem_benchmark/lineitem.duckdb');

ATTACH '/tmp/headless_duck_lineitem_benchmark/lineitem.duckdb' AS lineitem_file (READ_ONLY);

-- Full scan.
INSERT INTO hduck_lineitem_benchmark_events VALUES ('full_scan_hduck', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'full_scan',
	'hduck',
	count(*),
	sum(l_extendedprice * (1 - l_discount))::DECIMAL(38, 4),
	sum(l_quantity)::DECIMAL(38, 4),
	min(l_shipdate),
	max(l_shipdate)
FROM read_headlessduck('/tmp/headless_duck_lineitem_benchmark/lineitem.hduck');
INSERT INTO hduck_lineitem_benchmark_events VALUES ('full_scan_hduck', 'end', epoch_ms(current_timestamp));

INSERT INTO hduck_lineitem_benchmark_events VALUES ('full_scan_parquet', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'full_scan',
	'parquet',
	count(*),
	sum(l_extendedprice * (1 - l_discount))::DECIMAL(38, 4),
	sum(l_quantity)::DECIMAL(38, 4),
	min(l_shipdate),
	max(l_shipdate)
FROM read_parquet('/tmp/headless_duck_lineitem_benchmark/lineitem.parquet');
INSERT INTO hduck_lineitem_benchmark_events VALUES ('full_scan_parquet', 'end', epoch_ms(current_timestamp));

INSERT INTO hduck_lineitem_benchmark_events VALUES ('full_scan_duckdb', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'full_scan',
	'duckdb',
	count(*),
	sum(l_extendedprice * (1 - l_discount))::DECIMAL(38, 4),
	sum(l_quantity)::DECIMAL(38, 4),
	min(l_shipdate),
	max(l_shipdate)
FROM lineitem_file.lineitem;
INSERT INTO hduck_lineitem_benchmark_events VALUES ('full_scan_duckdb', 'end', epoch_ms(current_timestamp));

-- Projected aggregate scan.
INSERT INTO hduck_lineitem_benchmark_events VALUES ('projected_scan_hduck', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'projected_scan',
	'hduck',
	count(l_extendedprice),
	sum(l_extendedprice * (1 - l_discount))::DECIMAL(38, 4),
	NULL::DECIMAL(38, 4),
	NULL::DATE,
	NULL::DATE
FROM read_headlessduck('/tmp/headless_duck_lineitem_benchmark/lineitem.hduck');
INSERT INTO hduck_lineitem_benchmark_events VALUES ('projected_scan_hduck', 'end', epoch_ms(current_timestamp));

INSERT INTO hduck_lineitem_benchmark_events VALUES ('projected_scan_parquet', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'projected_scan',
	'parquet',
	count(l_extendedprice),
	sum(l_extendedprice * (1 - l_discount))::DECIMAL(38, 4),
	NULL::DECIMAL(38, 4),
	NULL::DATE,
	NULL::DATE
FROM read_parquet('/tmp/headless_duck_lineitem_benchmark/lineitem.parquet');
INSERT INTO hduck_lineitem_benchmark_events VALUES ('projected_scan_parquet', 'end', epoch_ms(current_timestamp));

INSERT INTO hduck_lineitem_benchmark_events VALUES ('projected_scan_duckdb', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'projected_scan',
	'duckdb',
	count(l_extendedprice),
	sum(l_extendedprice * (1 - l_discount))::DECIMAL(38, 4),
	NULL::DECIMAL(38, 4),
	NULL::DATE,
	NULL::DATE
FROM lineitem_file.lineitem;
INSERT INTO hduck_lineitem_benchmark_events VALUES ('projected_scan_duckdb', 'end', epoch_ms(current_timestamp));

-- TPC-H Q6-shaped filtered aggregate.
INSERT INTO hduck_lineitem_benchmark_events VALUES ('filtered_scan_hduck', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'filtered_scan',
	'hduck',
	count(*),
	sum(l_extendedprice * l_discount)::DECIMAL(38, 4),
	sum(l_quantity)::DECIMAL(38, 4),
	min(l_shipdate),
	max(l_shipdate)
FROM read_headlessduck('/tmp/headless_duck_lineitem_benchmark/lineitem.hduck')
WHERE
	l_shipdate >= DATE '1994-01-01'
	AND l_shipdate < DATE '1995-01-01'
	AND l_discount BETWEEN 0.05 AND 0.07
	AND l_quantity < 24;
INSERT INTO hduck_lineitem_benchmark_events VALUES ('filtered_scan_hduck', 'end', epoch_ms(current_timestamp));

INSERT INTO hduck_lineitem_benchmark_events VALUES ('filtered_scan_parquet', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'filtered_scan',
	'parquet',
	count(*),
	sum(l_extendedprice * l_discount)::DECIMAL(38, 4),
	sum(l_quantity)::DECIMAL(38, 4),
	min(l_shipdate),
	max(l_shipdate)
FROM read_parquet('/tmp/headless_duck_lineitem_benchmark/lineitem.parquet')
WHERE
	l_shipdate >= DATE '1994-01-01'
	AND l_shipdate < DATE '1995-01-01'
	AND l_discount BETWEEN 0.05 AND 0.07
	AND l_quantity < 24;
INSERT INTO hduck_lineitem_benchmark_events VALUES ('filtered_scan_parquet', 'end', epoch_ms(current_timestamp));

INSERT INTO hduck_lineitem_benchmark_events VALUES ('filtered_scan_duckdb', 'start', epoch_ms(current_timestamp));
INSERT INTO hduck_lineitem_benchmark_results
SELECT
	'filtered_scan',
	'duckdb',
	count(*),
	sum(l_extendedprice * l_discount)::DECIMAL(38, 4),
	sum(l_quantity)::DECIMAL(38, 4),
	min(l_shipdate),
	max(l_shipdate)
FROM lineitem_file.lineitem
WHERE
	l_shipdate >= DATE '1994-01-01'
	AND l_shipdate < DATE '1995-01-01'
	AND l_discount BETWEEN 0.05 AND 0.07
	AND l_quantity < 24;
INSERT INTO hduck_lineitem_benchmark_events VALUES ('filtered_scan_duckdb', 'end', epoch_ms(current_timestamp));

DETACH lineitem_file;

CREATE OR REPLACE TABLE hduck_lineitem_benchmark_timings AS
WITH pairs AS (
	SELECT
		label,
		max(CASE WHEN phase = 'start' THEN at_ms END) AS start_ms,
		max(CASE WHEN phase = 'end' THEN at_ms END) AS end_ms
	FROM hduck_lineitem_benchmark_events
	GROUP BY label
)
SELECT
	label,
	end_ms - start_ms AS elapsed_ms
FROM pairs
ORDER BY label;

CREATE OR REPLACE TABLE hduck_lineitem_benchmark_correctness AS
SELECT
	query_name,
	count(*) AS variants,
	count(DISTINCT row_count) = 1 AS row_count_matches,
	count(DISTINCT revenue) = 1 AS revenue_matches,
	CASE WHEN count(quantity_sum) = 0 THEN true ELSE count(DISTINCT quantity_sum) = 1 END AS quantity_sum_matches,
	CASE WHEN count(min_shipdate) = 0 THEN true ELSE count(DISTINCT min_shipdate) = 1 END AS min_shipdate_matches,
	CASE WHEN count(max_shipdate) = 0 THEN true ELSE count(DISTINCT max_shipdate) = 1 END AS max_shipdate_matches
FROM hduck_lineitem_benchmark_results
GROUP BY query_name
ORDER BY query_name;

.mode duckbox
.headers on

SELECT * FROM hduck_lineitem_benchmark_file_sizes ORDER BY source_format;
SELECT * FROM hduck_lineitem_benchmark_timings ORDER BY label;
SELECT * FROM hduck_lineitem_benchmark_results ORDER BY query_name, source_format;
SELECT * FROM hduck_lineitem_benchmark_correctness ORDER BY query_name;
