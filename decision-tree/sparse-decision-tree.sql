--------------------- JumpCalc ---------- START

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.jump_sfunc(INT[], INT, INT) CASCADE;
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.jump_sfunc(INT[], INT, INT) RETURNS INT[] AS $$
declare
	temp INT[];
begin
	temp = $1;
	temp[$2+1] = $3;
	RETURN temp;
end
$$ LANGUAGE plpgsql;

DROP AGGREGATE IF EXISTS MADLIB_SCHEMA.JumpCalc(INT, INT);
CREATE AGGREGATE MADLIB_SCHEMA.JumpCalc(INT, INT) (
  SFUNC=MADLIB_SCHEMA.jump_sfunc,
  STYPE=INT[]
);

---------------------- JumpCalc ---------- END

DROP TABLE IF EXISTS MADLIB_SCHEMA.tree2;
CREATE TABLE MADLIB_SCHEMA.tree2(
		id SERIAL,
		tree_location INT[],
		hash INT,
		feature INT,
		probability FLOAT,
		chisq FLOAT,
		maxclass INTEGER,
		infogain FLOAT,
		live INT,
		cat_size INT,
		parent_id INT,
		jump INT[]
) DISTRIBUTED BY (id);

DROP TABLE IF EXISTS MADLIB_SCHEMA.tree;
CREATE TABLE MADLIB_SCHEMA.tree(
		id SERIAL,
		tree_location INT[],
		hash INT,
		feature INT,
		probability FLOAT,
		chisq FLOAT,
		maxclass INTEGER,
		infogain FLOAT,
		live INT,
		cat_size INT,
		parent_id INT,
		jump INT[]
) DISTRIBUTED BY (id);

DROP TABLE IF EXISTS MADLIB_SCHEMA.finaltree;
CREATE TABLE MADLIB_SCHEMA.finaltree(
		id INT,
		new_id INT,
		parent_id INT
) DISTRIBUTED BY (id);

DROP TABLE IF EXISTS MADLIB_SCHEMA.finaltree2;
CREATE TABLE MADLIB_SCHEMA.finaltree2(
		id INT,
		new_id INT,
		parent_id INT
) DISTRIBUTED BY (id);

DROP FUNCTION IF EXISTS  MADLIB_SCHEMA.Classify_Tree(TEXT, INT);
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.Classify_Tree(table_name TEXT, num_values INT) RETURNS void AS $$
declare
	table_names TEXT[] = '{MADLIB_SCHEMA.classified_points1,MADLIB_SCHEMA.classified_points2}';
	jump_so_far INT := 0;
	old_jump_so_far INT := 0;
	table_pick INT := 1;
	old_depth INT := 0;
	new_depth INT := 0;
	remains_to_classify INT;
	size_finished INT;
begin
	DROP TABLE IF EXISTS MADLIB_SCHEMA.classified_points1;
	CREATE TABLE MADLIB_SCHEMA.classified_points1(
		id INT,
		feature MADLIB_SCHEMA.svec,
		jump INT,
		class INT,
		prob FLOAT
	) DISTRIBUTED BY (jump);
	
	DROP TABLE IF EXISTS MADLIB_SCHEMA.classified_points2;
	CREATE TABLE MADLIB_SCHEMA.classified_points2(
		id INT,
		feature MADLIB_SCHEMA.svec,
		jump INT,
		class INT,
		prob FLOAT
	) DISTRIBUTED BY (jump);
	
	DROP TABLE IF EXISTS MADLIB_SCHEMA.classified_points;
	CREATE TABLE MADLIB_SCHEMA.classified_points(
		id INT,
		feature MADLIB_SCHEMA.svec,
		jump INT,
		class INT,
		prob FLOAT
	) DISTRIBUTED BY (jump);

	EXECUTE 'INSERT INTO MADLIB_SCHEMA.classified_points1 (id, feature, jump, class, prob) SELECT id, feature, 1, 0, 0 FROM ' || table_name || ';'; 	
	LOOP
		EXECUTE 'SELECT id FROM MADLIB_SCHEMA.tree WHERE id > '|| jump_so_far ||' ORDER BY id LIMIT 1' INTO jump_so_far;
		RAISE INFO 'CLASSIFICATION STEP [%] CLASSIFIED: % TO BE CLASSIFIED: %', jump_so_far, size_finished, remains_to_classify;
		
		SELECT INTO new_depth array_upper(tree_location, 1) FROM MADLIB_SCHEMA.tree WHERE id = jump_so_far;
		IF(new_depth > old_depth) THEN
			EXECUTE 'INSERT INTO MADLIB_SCHEMA.classified_points SELECT * FROM '|| table_names[(table_pick)%2+1] ||' WHERE jump = 0;';
			EXECUTE 'TRUNCATE '|| table_names[(table_pick)%2+1] ||';';
			EXECUTE 'SELECT count(*) FROM MADLIB_SCHEMA.classified_points;' INTO size_finished;
			table_pick = table_pick%2+1; 
		END IF;
		old_depth = new_depth;
		
		EXECUTE 'SELECT count(*) FROM '|| table_names[(table_pick)%2+1] ||';' INTO remains_to_classify;
		IF ((jump_so_far IS NULL) OR (jump_so_far == old_jump_so_far) OR (remains_to_classify == 0)) THEN
			EXIT;
		END IF;
		old_jump_so_far = jump_so_far;

		EXECUTE 'INSERT INTO '|| table_names[table_pick] ||' SELECT pt.id, pt.feature, COALESCE(gt.jump[MADLIB_SCHEMA.svec_proj(pt.feature, gt.feature)+1],0), gt.maxclass, gt.probability FROM (SELECT * FROM '|| 
		table_names[(table_pick)%2+1] ||' WHERE jump = '|| jump_so_far ||') AS pt, (SELECT * FROM MADLIB_SCHEMA.tree WHERE id = '|| jump_so_far||') AS gt;';
	END LOOP;
	EXECUTE 'INSERT INTO MADLIB_SCHEMA.classified_points SELECT * FROM '|| table_names[table_pick] ||' WHERE jump = 0;';
	EXECUTE 'INSERT INTO MADLIB_SCHEMA.classified_points SELECT * FROM '|| table_names[table_pick%2+1] ||' WHERE jump = 0;';
end
$$ language plpgsql;

DROP FUNCTION IF EXISTS  MADLIB_SCHEMA.Cleanup_Tree(INT);
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.Cleanup_Tree(num_values INT) RETURNS void AS $$
declare
	tree_size INTEGER;
begin
	TRUNCATE MADLIB_SCHEMA.finaltree;
	TRUNCATE MADLIB_SCHEMA.finaltree2;
	DELETE FROM MADLIB_SCHEMA.tree2 WHERE COALESCE(cat_size,0) = 0;
	SELECT INTO tree_size count(*) FROM MADLIB_SCHEMA.tree2;
	INSERT INTO MADLIB_SCHEMA.finaltree (id, parent_id, new_id) SELECT id, MAX(parent_id), (tree_size+1) - count(1) OVER(ORDER BY id DESC ROWS UNBOUNDED PRECEDING) FROM MADLIB_SCHEMA.tree2 GROUP BY id;
	INSERT INTO MADLIB_SCHEMA.finaltree2 (id, parent_id, new_id) SELECT  g2.id,g.new_id,g2.new_id FROM MADLIB_SCHEMA.finaltree g, MADLIB_SCHEMA.finaltree g2  WHERE g.id = g2.parent_id;
	TRUNCATE MADLIB_SCHEMA.finaltree;
	TRUNCATE MADLIB_SCHEMA.tree;
	INSERT INTO MADLIB_SCHEMA.tree SELECT n.new_id, g.tree_location, g.hash, g.feature, g.probability, g.chisq, g.maxclass, g.infogain, g.live, g.cat_size, n.parent_id, g.jump FROM MADLIB_SCHEMA.tree2 g, MADLIB_SCHEMA.finaltree2 n WHERE n.id = g.id;
	INSERT INTO MADLIB_SCHEMA.tree SELECT * FROM MADLIB_SCHEMA.tree2 WHERE id = 1;
	TRUNCATE MADLIB_SCHEMA.tree2;
	INSERT INTO MADLIB_SCHEMA.tree2 (id, jump) SELECT parent_id, MADLIB_SCHEMA.JumpCalc(tree_location[array_upper(tree_location,1)], id) FROM MADLIB_SCHEMA.tree GROUP BY parent_id;
	TRUNCATE MADLIB_SCHEMA.finaltree2;
	UPDATE MADLIB_SCHEMA.tree k SET jump = g.jump FROM MADLIB_SCHEMA.tree2 g WHERE g.id = k.id;
	TRUNCATE MADLIB_SCHEMA.tree2;
end
$$ language plpgsql;

DROP FUNCTION IF EXISTS  MADLIB_SCHEMA.Train_Tree(TEXT, INT, INT);
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.Train_Tree(table_input TEXT, num_values INT, max_num_iter INT) RETURNS void AS $$
declare
	feature_dimention INT;
	dimensions FLOAT[];
	lifenodes INT;
	selection INT;
	sample_limit INT := 0;
	location INT[];
	temp_location INT[];
	num_classes INT;
	max_iter INT = max_num_iter;
	answer MADLIB_SCHEMA.res;
	location_size INT;
	max_id INT;
	flip INT := 1;
	category_size FLOAT[];
	category_class INT;
	table_names TEXT[] := '{MADLIB_SCHEMA.weighted_points,MADLIB_SCHEMA.weighted_points2}';
	time_stamp TIMESTAMP;
	time_stamp2 TIMESTAMP;
	misc_size INT;
begin	
	time_stamp2 = clock_timestamp();
	TRUNCATE MADLIB_SCHEMA.tree2;
	EXECUTE 'SELECT count(*) FROM '|| table_input ||';' INTO misc_size;
	RAISE INFO 'INPUT TABLE SIZE: %', misc_size;
	PERFORM MADLIB_SCHEMA.remove_redundent(table_input);
	EXECUTE 'SELECT count(*) FROM MADLIB_SCHEMA.weighted_points;' INTO misc_size;
	RAISE INFO 'TABLE SIZE AFTER COMPRESSION: %', misc_size;
	
	EXECUTE 'SELECT dimension(feature) FROM ' || table_names[1] || ' LIMIT 1;' INTO feature_dimention;
	EXECUTE 'SELECT COUNT(DISTINCT class) FROM ' || table_names[1] || ';' INTO num_classes; 
	
	EXECUTE 'INSERT INTO MADLIB_SCHEMA.tree2 (tree_location, hash, feature, probability, chisq, maxclass, infogain, live, cat_size, parent_id) VALUES(ARRAY[0], MADLIB_SCHEMA.hash_array(ARRAY[0]), 0, 1, 1, 1, 1, 1, 0, 0)';
	location_size = 0;
	
	LOOP
		SELECT INTO lifenodes COUNT(*) FROM MADLIB_SCHEMA.tree2 WHERE live = 1;
		IF((max_iter == 0) OR (lifenodes < 1)) THEN
			RAISE INFO 'EXIT: LIMIT % OR NO NODES LEFT', max_iter;
			EXIT;
		END IF;
		max_iter = max_iter-1;
		SELECT INTO selection id FROM MADLIB_SCHEMA.tree2 WHERE live = 1 ORDER BY id LIMIT 1;
		SELECT INTO max_id id FROM MADLIB_SCHEMA.tree2 WHERE live = 1 ORDER BY id DESC LIMIT 1;
		SELECT INTO location gt.tree_location FROM MADLIB_SCHEMA.tree2 gt WHERE gt.id = selection;
		IF(location_size < array_upper(location,1)) THEN
			flip = (flip)%2+1;
			location_size = array_upper(location,1);
			EXECUTE 'TRUNCATE TABLE ' || table_names[flip] || ';';
		END IF;
		
		EXECUTE 'SELECT ARRAY[COALESCE(count(*),0),COALESCE(sum(weight),0)] FROM ' || table_names[(flip%2)+1] || ' WHERE selection = ' || selection || ';' INTO category_size; 
		IF ((category_size[1] > 1) AND (category_size[2] > num_classes)) THEN
			RAISE INFO 'CURRENT SELECTION % CATEGORY SIZE %', selection, category_size[2];
			answer = MADLIB_SCHEMA.find_best_split(feature_dimention, num_classes, num_values, selection, sample_limit, table_names[(flip)%2+1]);
			time_stamp = clock_timestamp();
		 
			UPDATE MADLIB_SCHEMA.tree2 SET feature = answer.feature WHERE id = selection;
			UPDATE MADLIB_SCHEMA.tree2 SET probability = answer.probability WHERE id = selection;
			UPDATE MADLIB_SCHEMA.tree2 SET maxclass = answer.maxclass WHERE id = selection;
			UPDATE MADLIB_SCHEMA.tree2 SET infogain = answer.infogain WHERE id = selection;
			UPDATE MADLIB_SCHEMA.tree2 SET cat_size = category_size[2] WHERE id = selection; 
			UPDATE MADLIB_SCHEMA.tree2 SET live = 0 WHERE id = selection; 
			UPDATE MADLIB_SCHEMA.tree2 SET chisq = answer.chisq WHERE id = selection; 
		 
			IF (answer.live > 0) THEN --here insert live determination function 
				FOR i IN 0..num_values LOOP
		 			temp_location = location;
		 			temp_location[array_upper(temp_location,1)+1] = i;
		 			EXECUTE 'INSERT INTO MADLIB_SCHEMA.tree2 (tree_location, hash, feature, probability, maxclass, infogain, live, parent_id) VALUES(ARRAY[' || array_to_string(temp_location,',') || '],' ||
			 		 MADLIB_SCHEMA.hash_array(temp_location) || ', 0, 1, 1, 1, 1, '|| selection ||');';
		 		END LOOP;
		 		EXECUTE 'INSERT INTO ' || table_names[flip] || ' SELECT id, feature, class, weight, MADLIB_SCHEMA.svec_proj(feature,' || answer.feature || ') + ' || max_id+1 || ' FROM ' || 
		 		table_names[(flip%2)+1] || ' WHERE selection = ' || selection || ';';
			END IF; 
		ELSE
			UPDATE MADLIB_SCHEMA.tree2 SET live = 0 WHERE id = selection; 
			IF (category_size[2] > num_classes) THEN
				EXECUTE 'SELECT max(class) FROM ' || table_names[(flip%2)+1] || ' WHERE selection = ' || selection || ';' INTO category_class; 
				EXECUTE 'INSERT INTO MADLIB_SCHEMA.tree2 (tree_location, hash, feature, probability, maxclass, infogain, live, parent_id) VALUES(ARRAY[' || array_to_string(location,',') || '],' ||
			 		 MADLIB_SCHEMA.hash_array(temp_location) || ', 0, 1, 1, 1, 1, '|| selection ||');';
				UPDATE MADLIB_SCHEMA.tree2 SET feature = 1 WHERE id = selection;
				UPDATE MADLIB_SCHEMA.tree2 SET probability = 1.0 WHERE id = selection;
				UPDATE MADLIB_SCHEMA.tree2 SET chisq = 1.0 WHERE id = selection;
				UPDATE MADLIB_SCHEMA.tree2 SET maxclass = category_class WHERE id = selection;
				UPDATE MADLIB_SCHEMA.tree2 SET infogain = 0 WHERE id = selection; 
				UPDATE MADLIB_SCHEMA.tree2 SET cat_size = category_size[2] WHERE id = selection;
			ELSE
				DELETE FROM MADLIB_SCHEMA.tree2 WHERE id = selection;
			END IF;
		END IF;
	END LOOP;
	EXECUTE 'SELECT MADLIB_SCHEMA.Cleanup_Tree(' || num_values || ');';
	RAISE INFO '-------> FINAL TIME %' , (clock_timestamp() - time_stamp2);
end
$$ language plpgsql;

SELECT MADLIB_SCHEMA.Train_Tree('MADLIB_SCHEMA.Points',10, 3000);
SELECT * FROM MADLIB_SCHEMA.tree ORDER BY id;
SELECT MADLIB_SCHEMA.Classify_Tree('MADLIB_SCHEMA.Points', 10);