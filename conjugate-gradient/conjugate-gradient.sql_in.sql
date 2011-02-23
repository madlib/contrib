DROP FUNCTION IF EXISTS MADLIB_SCHEMA.vector_of(int4, float8);
CREATE FUNCTION MADLIB_SCHEMA.vector_of(int4, float8) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'vector_of'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_add_remove_null(float8[], float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_add_remove_null(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_add_remove_null'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_add(float8[], float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_add(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_add'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_sub(float8[], float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_sub(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_sub'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_mult(float8[], float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_mult(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_mult'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_div(float8[], float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_div(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_div'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_dif(float8[], float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_dif(float8[], float8[]) RETURNS FLOAT8
AS 'SVDlib.so', 'array_dif'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_sum(float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_sum(float8[]) RETURNS FLOAT8
AS 'SVDlib.so', 'array_sum'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_dot(float8[], float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_dot(float8[], float8[]) RETURNS FLOAT8
AS 'SVDlib.so', 'array_dot'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_scalar_mult(float8[], float8);
CREATE FUNCTION MADLIB_SCHEMA.array_scalar_mult(float8[], float8) RETURNS FLOAT8[]
AS 'SVDlib.so', 'array_scalar_mult'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_sqrt(float8[]);
CREATE FUNCTION MADLIB_SCHEMA.array_sqrt(float8[]) RETURNS FLOAT8[]
AS 'SVDlib.so', 'array_sqrt'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.array_limit(float8[], float8, float8);
CREATE FUNCTION MADLIB_SCHEMA.array_limit(float8[], float8, float8) RETURNS FLOAT8[]
AS 'SVDlib.so', 'array_limit'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS MADLIB_SCHEMA.conjugate_gradient(Matrix TEXT, val_id TEXT, row_id TEXT, b FLOAT[], precision_limit FLOAT) CASCADE;
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.conjugate_gradient(Matrix TEXT, val_id TEXT, row_id TEXT, b FLOAT[], precision_limit FLOAT)  RETURNS FLOAT[] AS $$
declare
	r FLOAT[];
	p FLOAT[];
	x FLOAT[];
	k INT;
	iter INT = 0;
	recidual_refresh INT := 30;
	alpha FLOAT;
	r_size FLOAT;
	r_new_size FLOAT;
	Ap FLOAT[];
	Ax FLOAT[];
	pAp_size FLOAT;
	beta FLOAT;
begin	
	SELECT INTO k array_upper(b,1);
	SELECT INTO x ARRAY(SELECT random() FROM generate_series(1, k));
	LOOP
		IF(iter%recidual_refresh = 0)THEN 
			EXECUTE 'SELECT ARRAY(SELECT MADLIB_SCHEMA.array_dot('||val_id||', ARRAY[' || array_to_string(x,',') || ']) FROM '|| Matrix ||' ORDER BY '||row_id||' LIMIT '|| k ||')' INTO Ax;
			SELECT INTO r array_sub(b, Ax);
			SELECT INTO r_size array_dot(r, r);
			RAISE INFO 'COMPUTE RESIDUAL ERROR %', r_size;
			SELECT INTO p r; 
		END IF;
		iter = iter + 1;
		EXECUTE 'SELECT ARRAY(SELECT MADLIB_SCHEMA.array_dot('||val_id||', ARRAY[' || array_to_string(p,',') || ']) FROM '|| Matrix ||' ORDER BY '||row_id||' LIMIT '|| k ||')' INTO Ap;
		SELECT INTO pAp_size array_dot(p, Ap);
		alpha = r_size/pAp_size;
		SELECT INTO x MADLIB_SCHEMA.array_add(x, MADLIB_SCHEMA.array_scalar_mult(p,alpha));
		SELECT INTO r MADLIB_SCHEMA.array_add(r, MADLIB_SCHEMA.array_scalar_mult(Ap, -alpha));
		SELECT INTO r_new_size MADLIB_SCHEMA.array_dot(r,r);
		RAISE INFO 'ERROR %',r_new_size; 
		IF (r_new_size < precision_limit) THEN
			EXECUTE 'SELECT ARRAY(SELECT MADLIB_SCHEMA.array_dot('||val_id||', ARRAY[' || array_to_string(x,',') || ']) FROM '|| Matrix ||' ORDER BY '||row_id||' LIMIT '|| k ||')' INTO Ax;
			SELECT INTO r MADLIB_SCHEMA.array_sub(b, Ax);
			SELECT INTO r_new_size MADLIB_SCHEMA.array_dot(r, r);
			RAISE INFO 'TEST FINAL ERROR %', r_new_size;
			IF (r_new_size < precision_limit) THEN
				EXIT;
			END IF;
		END IF;
		SELECT INTO p MADLIB_SCHEMA.array_add(r, MADLIB_SCHEMA.array_scalar_mult(p, r_new_size/r_size));
		r_size = r_new_size;
	END LOOP; 
	RETURN x;
end
$$ LANGUAGE plpgsql;
