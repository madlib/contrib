DROP FUNCTION IF EXISTS CG(Matrix TEXT, val_id TEXT, row_id TEXT, b FLOAT[], precision_limit FLOAT) CASCADE;
CREATE OR REPLACE FUNCTION CG(Matrix TEXT, val_id TEXT, row_id TEXT, b FLOAT[], precision_limit FLOAT)  RETURNS FLOAT[] AS $$
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
			EXECUTE 'SELECT ARRAY(SELECT array_dot('||val_id||', ARRAY[' || array_to_string(x,',') || ']) FROM '|| Matrix ||' ORDER BY '||row_id||' LIMIT '|| k ||')' INTO Ax;
			SELECT INTO r array_sub(b, Ax);
			SELECT INTO r_size array_dot(r, r);
			RAISE INFO 'COMPUTE RESIDUAL ERROR %', r_size;
			SELECT INTO p r; 
		END IF;
		iter = iter + 1;
		EXECUTE 'SELECT ARRAY(SELECT array_dot('||val_id||', ARRAY[' || array_to_string(p,',') || ']) FROM '|| Matrix ||' ORDER BY '||row_id||' LIMIT '|| k ||')' INTO Ap;
		SELECT INTO pAp_size array_dot(p, Ap);
		alpha = r_size/pAp_size;
		SELECT INTO x array_add(x, array_scalar_mult(p,alpha));
		SELECT INTO r array_add(r,array_scalar_mult(Ap, -alpha));
		SELECT INTO r_new_size array_dot(r,r);
		RAISE INFO 'ERROR %',r_new_size; 
		IF (r_new_size < precision_limit) THEN
			EXECUTE 'SELECT ARRAY(SELECT array_dot('||val_id||', ARRAY[' || array_to_string(x,',') || ']) FROM '|| Matrix ||' ORDER BY '||row_id||' LIMIT '|| k ||')' INTO Ax;
			SELECT INTO r array_sub(b, Ax);
			SELECT INTO r_new_size array_dot(r, r);
			RAISE INFO 'TEST FINAL ERROR %', r_new_size;
			IF (r_new_size < precision_limit) THEN
				EXIT;
			END IF;
		END IF;
		SELECT INTO p array_add(r, array_scalar_mult(p, r_new_size/r_size));
		r_size = r_new_size;
	END LOOP; 
	RETURN x;
end
$$ LANGUAGE plpgsql;
