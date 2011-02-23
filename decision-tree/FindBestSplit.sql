--------------------- MADLIB_SCHEMA.MADLIB_SCHEMA.findentropy ---------- START

-- FindInfoGain must return Info Gain, Gain Significance and Probability of main class

DROP TYPE IF EXISTS MADLIB_SCHEMA.findinfogain_type CASCADE;
CREATE TYPE MADLIB_SCHEMA.findinfogain_type AS(
  value FLOAT[],
  num_class INT,
  num_values INT,
  dim INT
);

DROP TYPE IF EXISTS MADLIB_SCHEMA.findinfogain_result CASCADE;
CREATE TYPE MADLIB_SCHEMA.findinfogain_result AS(
  value1 FLOAT,
  value2 FLOAT,
  value3 FLOAT,
  value4 FLOAT,
  value5 FLOAT,
  value6 FLOAT
);

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.findinfogain_sfunc(MADLIB_SCHEMA.findinfogain_type, FLOAT, FLOAT, INT, INT, INT, INT) CASCADE;
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.findinfogain_sfunc(MADLIB_SCHEMA.findinfogain_type, FLOAT, FLOAT, INT, INT, INT, INT) RETURNS MADLIB_SCHEMA.findinfogain_type AS $$
begin
	IF(($1.num_class IS NULL) OR ($1.num_class < 2)) THEN
		$1.value = MADLIB_SCHEMA.mallocset(($4+1)*($5+1),0);
		$1.num_class = $4;
		$1.num_values = $5;
		$1.dim = $7;
	END IF;
	$1.value = MADLIB_SCHEMA.aggr_InfoGain($1.value, $2, $3, $4, $5, $6); 
	RETURN $1;
end
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.findinfogain_prefunc(MADLIB_SCHEMA.findinfogain_type, MADLIB_SCHEMA.findinfogain_type) CASCADE;
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.findinfogain_prefunc(MADLIB_SCHEMA.findinfogain_type, MADLIB_SCHEMA.findinfogain_type) RETURNS MADLIB_SCHEMA.findinfogain_type AS $$
begin
	IF(($1.num_class IS NOT NULL) AND ($2.num_class IS NOT NULL)) THEN
		$1.value = MADLIB_SCHEMA.array_add($1.value, $2.value);
		RETURN $1;
	END IF;
	
	IF($1.num_class IS NOT NULL) THEN
		RETURN $1;
	ELSE
		RETURN $2;
	END IF;
end
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.findinfogain_finalfunc(MADLIB_SCHEMA.findinfogain_type) CASCADE;
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.findinfogain_finalfunc(MADLIB_SCHEMA.findinfogain_type) RETURNS MADLIB_SCHEMA.findinfogain_result AS $$
declare
	temp FLOAT[];
	result MADLIB_SCHEMA.findinfogain_result;
begin
	IF($1.value IS NOT NULL) THEN
		temp = MADLIB_SCHEMA.compute_InfoGain($1.value, $1.num_class, $1.num_values);
		result.value1 = COALESCE($1.dim,0);
		result.value2 = temp[1];
		result.value3 = temp[2];
		result.value4 = temp[3];
		result.value5 = temp[4];
		result.value6 = $1.value[1];
	ELSE
		result.value1 = COALESCE($1.dim, 0);
		result.value2 = 0;
		result.value3 = 1;
		result.value4 = 1;
		result.value5 = 0;
		result.value6 = 0;
	END IF;
	return result;
end
$$ LANGUAGE plpgsql;

DROP AGGREGATE IF EXISTS MADLIB_SCHEMA.FindInfoGain(FLOAT, FLOAT, INT, INT, INT, INT);
CREATE AGGREGATE MADLIB_SCHEMA.FindInfoGain(FLOAT, FLOAT, INT, INT, INT, INT) (
  SFUNC=MADLIB_SCHEMA.findinfogain_sfunc,
  PREFUNC = MADLIB_SCHEMA.findinfogain_prefunc,
  FINALFUNC=MADLIB_SCHEMA.findinfogain_finalfunc,
  STYPE=MADLIB_SCHEMA.findinfogain_type
);

---------------------- MADLIB_SCHEMA.MADLIB_SCHEMA.findentropy ---------- END
DROP TYPE IF EXISTS MADLIB_SCHEMA.res CASCADE;
CREATE TYPE MADLIB_SCHEMA.res AS(
	feature INT,
	probability FLOAT,
	maxclass INTEGER,
	infogain FLOAT,
	live INT,
	chisq FLOAT
);

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.find_best_split(INT, INT, INT, INT, INT, TEXT);
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.find_best_split(feature_dimentions INT, distinct_classes INT, distinct_features INT, selection INT, sample_limit INT, table_name TEXT) RETURNS MADLIB_SCHEMA.res AS $$
declare
	sample_dimentions INT;
	selected_dimentions INT[];
	tablesize INT;
	i INT;
	new_sample_limit FLOAT := sample_limit;
	pre_result FLOAT[];
	result MADLIB_SCHEMA.res;
	vdebug FLOAT[];
	hdebug INT;
	total_size INT;
begin	 
	--this computes how many dimentions need to samples to find one that is in 90th percentile with
	-- .999 probability.
	sample_dimentions = min(floor(-ln(1-(.999)^(1/CAST(10 AS FLOAT)))*10),feature_dimentions);
	selected_dimentions = MADLIB_SCHEMA.WeightedNoReplacement(sample_dimentions, feature_dimentions);
	
	EXECUTE 'SELECT count(*) FROM ' || table_name || ' WHERE selection = ' || selection || ';' INTO total_size;
	IF (new_sample_limit == 0) THEN
		new_sample_limit = total_size;
	END IF;
	
	DROP TABLE IF EXISTS selectedDimentionTableResults;
	CREATE TEMP TABLE selectedDimentionTableResults( -- make temp
		dim INT,
		infoGain FLOAT,
		gainSign FLOAT,
		classProb FLOAT,
		classID FLOAT,
		relativeSize FLOAT
	) DISTRIBUTED BY (dim);
	
	DROP TABLE IF EXISTS selectedDimentionTable;
	CREATE TEMP TABLE selectedDimentionTable(
		dim INT
	) DISTRIBUTED BY (dim);
	
	EXECUTE 'INSERT INTO selectedDimentionTable SELECT distinct (ARRAY[ ' || array_to_string(selected_dimentions, ',') || 
	'])[g.a] FROM generate_series(1,' || array_upper(selected_dimentions ,1) || ') AS g(a);';
	EXECUTE 'SELECT count(*) FROM selectedDimentionTable' INTO tablesize;
	
	EXECUTE 'INSERT INTO selectedDimentionTableResults SELECT (g.t).* FROM (SELECT MADLIB_SCHEMA.FindInfoGain(MADLIB_SCHEMA.svec_proj(wp.feature, dt.dim), wp.weight, ' || distinct_classes ||
	', ' || distinct_features || ', wp.class, dt.dim) AS t FROM (SELECT w.feature, w.weight, w.class FROM ' || 
	table_name || ' w WHERE w.selection = ' || selection || ' LIMIT ' || new_sample_limit || ') AS wp CROSS JOIN (SELECT * FROM ' || 
	' selectedDimentionTable) AS dt WHERE MADLIB_SCHEMA.svec_proj(wp.feature, dt.dim) > 0 GROUP BY dt.dim) AS g;';
	
	EXECUTE 'SELECT ARRAY[dt.dim, dt.infoGain, dt.gainSign, dt.classProb, dt.classID, dt.relativeSize] FROM  selectedDimentionTableResults dt, (SELECT max(infoGain) AS maxClass FROM selectedDimentionTableResults) AS m WHERE dt.infoGain = m.maxClass AND dt.classID > 0 LIMIT 1' INTO pre_result;
		
	result.feature = pre_result[1];
	result.maxclass = pre_result[5];
	EXECUTE 'SELECT count(*) FROM ' || table_name || ' WHERE selection = ' || selection || ' AND class = '|| result.maxclass ||';' INTO result.probability;
	result.probability = result.probability/total_size;
	result.infogain = pre_result[2];
	result.chisq = MADLIB_SCHEMA.chi2pdf(pre_result[3], distinct_features-1);
	
	IF ((result.chisq < 0.5/sample_dimentions) OR (MADLIB_SCHEMA.chi2pdf((pre_result[6]-total_size)*(pre_result[6]-total_size)/total_size, 1) < .1)) THEN
		result.live = 1;
	ELSE
		result.live = 0;
	END IF;
	
	RETURN result;
end
$$ language plpgsql;