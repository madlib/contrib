---------------------- CreateHash ---------- START

DROP TYPE IF EXISTS MADLIB_SCHEMA.hash_val CASCADE;
CREATE TYPE MADLIB_SCHEMA.hash_val AS(
	id INTEGER,
	feature MADLIB_SCHEMA.svec,
	class INTEGER,
	weight INTEGER,
	selection INTEGER 
);

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.weight_count(MADLIB_SCHEMA.hash_val, int, MADLIB_SCHEMA.svec, int) CASCADE;
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.weight_count(MADLIB_SCHEMA.hash_val, int, MADLIB_SCHEMA.svec, int) RETURNS MADLIB_SCHEMA.hash_val AS $$
declare
begin

IF ($1.weight IS NOT NULL) THEN
	$1.weight = $1.weight + 1;
ELSE
	$1.weight = 1;
	$1.feature = $3;
	$1.id = $2;
	$1.class = $4;
	$1.selection = 1;
END IF;

RETURN $1;
end
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.weight_aggr(MADLIB_SCHEMA.hash_val, MADLIB_SCHEMA.hash_val) CASCADE;
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.weight_aggr(MADLIB_SCHEMA.hash_val, MADLIB_SCHEMA.hash_val) RETURNS MADLIB_SCHEMA.hash_val AS $$
declare
begin

IF ($2.id IS NOT NULL) THEN
	$1.id = $2.id;
	$1.feature = $2.feature;
	$1.class = $2.class;
	$1.weight = COALESCE($1.weight, 0) + COALESCE($2.weight, 0);
	$1.selection = 1;
END IF;

RETURN $1;
end
$$ LANGUAGE plpgsql;

DROP AGGREGATE IF EXISTS MADLIB_SCHEMA.CreateHash(int, MADLIB_SCHEMA.svec, int);
CREATE AGGREGATE MADLIB_SCHEMA.CreateHash(int, MADLIB_SCHEMA.svec, int) (
  SFUNC=MADLIB_SCHEMA.weight_count,
  PREFUNC=MADLIB_SCHEMA.weight_aggr,
  STYPE=MADLIB_SCHEMA.hash_val
);

---------------------- CreateHash ---------- END

CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.remove_redundent(table_input TEXT) RETURNS void AS $$
begin	
	DROP TABLE IF EXISTS MADLIB_SCHEMA.weighted_points;
	CREATE TABLE MADLIB_SCHEMA.weighted_points(
		id INTEGER,
		feature MADLIB_SCHEMA.svec,
		class INTEGER,
		weight INTEGER,
		selection INTEGER
	) DISTRIBUTED BY (selection);
	
	DROP TABLE IF EXISTS MADLIB_SCHEMA.weighted_points2;
	CREATE TABLE MADLIB_SCHEMA.weighted_points2(
		id INTEGER,
		feature MADLIB_SCHEMA.svec,
		class INTEGER,
		weight INTEGER,
		selection INTEGER
	) DISTRIBUTED BY (selection);
	
	EXECUTE 'INSERT INTO MADLIB_SCHEMA.weighted_points SELECT (MADLIB_SCHEMA.CreateHash(id, feature, class)).* FROM (SELECT id, feature, class, hash(feature) as hash FROM '|| table_input ||') as A GROUP BY A.hash';
end
$$ language plpgsql;