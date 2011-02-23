DROP FUNCTION IF EXISTS vector_of(int4, float8);
CREATE FUNCTION vector_of(int4, float8) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'vector_of'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_add_remove_null(float8[], float8[]);
CREATE FUNCTION array_add_remove_null(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_add_remove_null'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_add(float8[], float8[]);
CREATE FUNCTION array_add(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_add'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_sub(float8[], float8[]);
CREATE FUNCTION array_sub(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_sub'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_mult(float8[], float8[]);
CREATE FUNCTION array_mult(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_mult'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_div(float8[], float8[]);
CREATE FUNCTION array_div(float8[], float8[]) RETURNS FLOAT8[] 
AS 'SVDlib.so', 'array_div'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_dif(float8[], float8[]);
CREATE FUNCTION array_dif(float8[], float8[]) RETURNS FLOAT8
AS 'SVDlib.so', 'array_dif'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_sum(float8[]);
CREATE FUNCTION array_sum(float8[]) RETURNS FLOAT8
AS 'SVDlib.so', 'array_sum'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_dot(float8[], float8[]);
CREATE FUNCTION array_dot(float8[], float8[]) RETURNS FLOAT8
AS 'SVDlib.so', 'array_dot'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_scalar_mult(float8[], float8);
CREATE FUNCTION array_scalar_mult(float8[], float8) RETURNS FLOAT8[]
AS 'SVDlib.so', 'array_scalar_mult'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_sqrt(float8[]);
CREATE FUNCTION array_sqrt(float8[]) RETURNS FLOAT8[]
AS 'SVDlib.so', 'array_sqrt'
LANGUAGE C IMMUTABLE;

DROP FUNCTION IF EXISTS array_limit(float8[], float8, float8);
CREATE FUNCTION array_limit(float8[], float8, float8) RETURNS FLOAT8[]
AS 'SVDlib.so', 'array_limit'
LANGUAGE C IMMUTABLE;