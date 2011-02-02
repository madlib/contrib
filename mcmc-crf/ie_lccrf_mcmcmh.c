#include "postgres.h"
#include "executor/executor.h" // needed for composite-Type Arguments
#include "funcapi.h" // needed for returning rows (composite types)
#include <string.h>
#include "fmgr.h"
#include "utils/array.h"
#include <math.h>
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */ 
// #include "arrayutils.h"

PG_MODULE_MAGIC;

Datum getscore(PG_FUNCTION_ARGS);
Datum getalpha(PG_FUNCTION_ARGS);
Datum SimpleQuery1(PG_FUNCTION_ARGS);

// NOTE: postgres already called srand48. it also uses lrand48 as random().
// we call drand48 directly instead using postgres random() wrapper
// see random.h from postgres
//

/***********************************************************************
 * getscore(prevlabel, label, score_array, domain_size)
 * Retrive score from the score_array given indexes: (prevlabel, label) and the domain_size
 * Note: prevlabel could be null
 
 * create function getscore(integer, integer, anyarray, integer) returns integer as '/home/daisyw/p84/mingw-postgresql-8.4dev/src/bayesstore/ie_lccrf_mcmcmh', 'getscore' language c called on null input;
 
 ************************************************************************/

PG_FUNCTION_INFO_V1(getscore);

Datum
getscore(PG_FUNCTION_ARGS)
{
	int32		v1, v2; // prevlabel, label
	ArrayType  	*v3; // score_array
	int32		v4; // domain_size
	int 		nitems;
	int32 		score = 0; 
    int         spos;

elog(DEBUG1, "***IN GETSCORE***");

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		abort();

	v2 = PG_GETARG_INT32(1);
	v3 = PG_GETARG_ARRAYTYPE_P(2);
	v4 = PG_GETARG_INT32(3);

	// number of items in the arrays
	nitems = ARR_DIMS(v3)[0];
	spos = nitems % (v4*v4);

//elog(DEBUG1, "prevlabel %d, label %d, domain_size %d, nitems %d, spos %d", v1, v2, v4, nitems, spos);

	// first token case
 	if (PG_ARGISNULL(0)) {
elog(DEBUG1, "first param is null"); 
	 	if (spos!=v4) abort();	
		score = ((int*)ARR_DATA_PTR(v3))[0*v4+v2-1];
elog(DEBUG1, "score = %d", score); 
		PG_RETURN_INT32(score);
	}

	v1 = PG_GETARG_INT32(0);

	// otherwise
	score = ((int*)ARR_DATA_PTR(v3))[spos+(v1-1)*v4+v2-1];
//elog(DEBUG1, "score = %d", score); 
  	PG_RETURN_INT32(score);
}

#include "getref.incl.c"

/******************************************************
 * getalpha
 *
 * create function getalpha(getalpha_io, getalpha_io) returns getalpha_io as '/home/daisyw/p84/mingw-postgresql-8.4dev/src/bayesstore/ie_lccrf_mcmcmh', 'getalpha' language c called on null input;
 *
 * ****************************************************/

typedef struct getalpha_io
{
	int		doc_id;
	int		start_pos;
	int		label;
	double		alpha;
	int		*cur;
	int		*factors;
	int		*factor_lens;
} getalpha_io;

// global variables
//static TupleDesc	tupdesc = NULL;
//static int		counter = 0;

PG_FUNCTION_INFO_V1(getalpha);

Datum
getalpha(PG_FUNCTION_ARGS)
{
	bool 		isnull;
	Datum		input;
	Datum		inputref;
	bool		isupdate= false;	// is initial running state

	int32		v1_doc_id, v1_start_pos, v1_label;
	float4		v1_alpha;

	int32		v2_doc_id, v2_start_pos, v2_label;
	float4		v2_alpha;

	ArrayType	*cur;
	ArrayType	*factors;
	ArrayType	*factor_lens;

	int32		r_doc_id, r_start_pos, r_label;
	float4		r_alpha;
	ArrayType	*next;
	ArrayType	*r_factors;
	ArrayType	*r_factor_lens;

	TupleDesc	tupdesc;
	HeapTuple	tup;
	Datum		values[7];
	bool		nulls[7] = {0,0,0,0,0,0,0};
	Datum		result;

	int32		olabel;
	int32		ndims;
	int		i;
	double		r;

//elog(DEBUG1, "***IN new GETALPHA***");

	if (PG_ARGISNULL(1))
		abort();

	// get new aggregate input
        HeapTupleHeader v2 = PG_GETARG_HEAPTUPLEHEADER(1);
	input = GetAttributeByName(v2, "doc_id", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_doc_id = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_start_pos = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "label", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_label = DatumGetInt32(input); 
	//input = GetAttributeByName(v2, "alpha", &isnull);
	//if (isnull) 
	//	PG_RETURN_NULL();
	//v2_alpha = DatumGetFloat4(input); 
	v2_alpha = 0.0; 
	input = GetAttributeByName(v2, "cur", &isnull);
	if (isnull)
		cur = NULL;
	else {
		isupdate = true;
		cur = DatumGetArrayTypeP(input);
		ndims = ARR_DIMS(cur)[0];
//elog(DEBUG1, "v2_cur_len: %d", ndims); 
//elog(DEBUG1, "v2_cur[36-39]: %d,%d,%d,%d",((int*)ARR_DATA_PTR(cur))[36],((int*)ARR_DATA_PTR(cur))[37],((int*)ARR_DATA_PTR(cur))[38],((int*)ARR_DATA_PTR(cur))[39]); 
	}
	input = GetAttributeByName(v2, "factors", &isnull);
	if (isnull)
		factors = NULL;
	else 
		factors = DatumGetArrayTypeP(input);
	input = GetAttributeByName(v2, "factor_lens", &isnull);
	if (isnull)
		factor_lens = NULL;
	else 
		factor_lens = DatumGetArrayTypeP(input);

elog(DEBUG1, "[new state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, v2_label);

	if (PG_ARGISNULL(0) || isupdate==true) {
//elog(DEBUG1, "***Update State***");

		//TODO: logics
		
		if (cur == NULL || factors == NULL || factor_lens == NULL)		
			PG_RETURN_NULL();

		// construct a new composite type 
		// as result running state
		r_doc_id = v2_doc_id;
		values[0] = Int32GetDatum(r_doc_id); 
		r_start_pos = v2_start_pos;
		values[1] = Int32GetDatum(r_start_pos);
		r_label = v2_label;
		values[2] = Int32GetDatum(r_label);

		next = DatumGetArrayTypePCopy(cur);
		r_factors = DatumGetArrayTypePCopy(factors);
		r_factor_lens= DatumGetArrayTypePCopy(factor_lens);

		values[4] = PointerGetDatum(next); 
		values[5] = PointerGetDatum(r_factors);
		values[6] = PointerGetDatum(r_factor_lens);

	// compute alpha 
	{
		olabel = ((int*)ARR_DATA_PTR(cur))[(ndims/3)*2+r_start_pos];
	      	int nitems = ((int*)ARR_DATA_PTR(r_factor_lens))[r_start_pos];	
		int nlabels = sqrt(nitems);
		int spos = nitems % (nlabels*nlabels);
		int index = 0;
		int plabel;
		int newpi, oldpi;
elog(DEBUG1, "[old state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, olabel);

		if (r_start_pos == 0) {
			newpi = ((int*)ARR_DATA_PTR(r_factors))[index+r_label-1];
//elog(DEBUG1, "[newpi] index: %d, value: %d", (index+r_label-1), newpi);
			oldpi = ((int*)ARR_DATA_PTR(r_factors))[index+olabel-1];
//elog(DEBUG1, "[oldpi] index: %d, value: %d", (index+olabel-1), oldpi);
		} else {
			plabel = ((int*)ARR_DATA_PTR(cur))[(ndims/3)*2+r_start_pos-1];
elog(DEBUG1, "prevlabel: %d", plabel);
			for (i=0; i<r_start_pos; i++)
				index += ((int*)ARR_DATA_PTR(r_factor_lens))[i]; 
			newpi = ((int*)ARR_DATA_PTR(r_factors))[index+spos+(plabel-1)*nlabels+r_label-1];
//elog(DEBUG1, "[newpi] index: %d, value: %d", (index+spos+(plabel-1)*(ndims/3)+r_label-1), newpi);
			oldpi = ((int*)ARR_DATA_PTR(r_factors))[index+spos+(plabel-1)*nlabels+olabel-1];
//elog(DEBUG1, "[oldpi] index: %d, value: %d", (index+spos+(plabel-1)*(ndims/3)+olabel-1), oldpi);
		}
elog(DEBUG1, "newpi, oldpi, r_alpha: %d, %d, %f", newpi, oldpi,r_alpha);

		if (r_start_pos < (ndims/3-1)) {
			nitems = ((int*)ARR_DATA_PTR(r_factor_lens))[r_start_pos+1];
			spos = nitems % (nlabels*nlabels);
			plabel = ((int*)ARR_DATA_PTR(cur))[(ndims/3)*2+r_start_pos+1];
elog(DEBUG1, "postlabel: %d", plabel);
			int index2 = ((int*)ARR_DATA_PTR(r_factor_lens))[r_start_pos];
			newpi = newpi + ((int*)ARR_DATA_PTR(r_factors))[index+index2+spos+(r_label-1)*nlabels+plabel-1];
			oldpi = oldpi + ((int*)ARR_DATA_PTR(r_factors))[index+index2+spos+(olabel-1)*nlabels+plabel-1];
		}

		r_alpha = exp(((float)(newpi-oldpi))/1000.0);
		values[3] = Float4GetDatum(r_alpha);
elog(DEBUG1, "newpi, oldpi, r_alpha: %d, %d, %f", newpi, oldpi,r_alpha);
//		if (r_alpha>1.0)	r_alpha = 1.0;
	}

 	//r = (((double)rand())/(((double)RAND_MAX)+((double)1))); 
 	r = drand48();
elog(DEBUG1, "random: %f", r);

/* for computing distribution */
	if (r < r_alpha) {
if(r_label != olabel)
elog(DEBUG1, "accept the proposal values", r);

/* for computing MAX */
//	if (r_alpha > 1) {

	// update state
	// TODO: get rid of doc_id and start_pos
	// -- no use
	((int*)ARR_DATA_PTR(next))[(ndims/3)*2+r_start_pos]=r_label;

	} else {
elog(DEBUG1, "fill in dummy values", r);

	// fill in dummy values 
	r_doc_id = -1;
	values[0] = Int32GetDatum(r_doc_id); 
	r_start_pos = -1;
	values[1] = Int32GetDatum(r_start_pos);
	r_label = -1;
	values[2] = Int32GetDatum(r_label);
	}

		// build a tuple descriptor for our result type 
		if (/*((counter++)%10000==0 || tupdesc == NULL) &&*/ get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE){
			ereport(ERROR,
       	             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
       	              errmsg("function returning record called in context that cannot accept type record")));
			tupdesc = BlessTupleDesc(tupdesc);
		}

//		for (i=4; i<(tupdesc->natts); i++) {
//			nulls[i] = 1;
//elog(DEBUG1, "nulls[%d]: %d", i, nulls[i]);
//		}
//elog(DEBUG1, "tupdesc->natts: %d", tupdesc->natts);

		tup = heap_form_tuple(tupdesc, values, nulls);

		result = HeapTupleGetDatum(tup);

   		PG_RETURN_DATUM(result);
	}

	// get running aggregate state 
        HeapTupleHeader v1 = PG_GETARG_HEAPTUPLEHEADER(0);
	input = GetAttributeByName(v1, "doc_id", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_doc_id = DatumGetInt32(input); 
	input = GetAttributeByName(v1, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_start_pos = DatumGetInt32(input); 
	input = GetAttributeByName(v1, "label", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_label = DatumGetInt32(input); 
	//input = GetAttributeByName(v1, "alpha", &isnull);
	//if (isnull) 
	//	PG_RETURN_NULL();
	//v1_alpha = DatumGetFloat4(input); 
	v1_alpha=0.0;
	input = GetAttributeByName(v1, "cur", &isnull);
	if (isnull)  
		PG_RETURN_NULL();
	else {
		cur = DatumGetArrayTypeP(input);
		ndims = ARR_DIMS(cur)[0];
//elog(DEBUG1, "v1_cur_len: %d", ndims);
//elog(DEBUG1, "v1_cur[0-3]: %d,%d,%d,%d",((int*)ARR_DATA_PTR(cur))[0],((int*)ARR_DATA_PTR(cur))[1],((int*)ARR_DATA_PTR(cur))[2],((int*)ARR_DATA_PTR(cur))[3]); 
	}
	input = GetAttributeByName(v1, "factors", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	else 
		factors = DatumGetArrayTypeP(input);
	input = GetAttributeByName(v1, "factor_lens", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	else 
		factor_lens = DatumGetArrayTypeP(input);

//elog(DEBUG1, "[v1] doc_id: %d, start_pos: %d, label: %d", v1_doc_id, v1_start_pos, v1_label);

	// fill in values of the new running state
	r_doc_id = v2_doc_id;
	inputref = GetAttributeByRef(v1, "doc_id", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_doc_id;
	r_start_pos = v2_start_pos;
	inputref = GetAttributeByRef(v1, "start_pos", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_start_pos;
	r_label = v2_label;
	inputref = GetAttributeByRef(v1, "label", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_label;

	// compute new alpha
//elog(DEBUG1, "***compute new alpha***");
	{
		olabel = ((int*)ARR_DATA_PTR(cur))[(ndims/3)*2+r_start_pos];
//elog(DEBUG1, "factor_lens, r_start_pos: %d, %d", ARR_DIMS(factor_lens)[0], r_start_pos);
	      	int nitems = ((int*)ARR_DATA_PTR(factor_lens))[r_start_pos];	
		int nlabels = sqrt(nitems);
		int spos = nitems % (nlabels*nlabels);
		int index = 0;
		int plabel;
		int newpi, oldpi;
elog(DEBUG1, "[old state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, olabel);

		if (r_start_pos == 0) {
			newpi = ((int*)ARR_DATA_PTR(factors))[index+r_label-1];
//elog(DEBUG1, "[newpi] index: %d, value: %d", (index+r_label-1), newpi);
			oldpi = ((int*)ARR_DATA_PTR(factors))[index+olabel-1];
//elog(DEBUG1, "[oldpi] index: %d, value: %d", (index+olabel-1), oldpi);
		} else {
			plabel = ((int*)ARR_DATA_PTR(cur))[(ndims/3)*2+r_start_pos-1];
elog(DEBUG1, "prevlabel2: %d", plabel);
			for (i=0; i<r_start_pos; i++)
				index += ((int*)ARR_DATA_PTR(factor_lens))[i]; 
//elog(DEBUG1, "r_start_pos, base_index, spos, plabel, ndims/3, r_label:%d, %d, %d, %d, %d, %d", r_start_pos, index, spos, plabel, ndims/3, r_label);
			newpi = ((int*)ARR_DATA_PTR(factors))[index+spos+(plabel-1)*nlabels+r_label-1];
//elog(DEBUG1, "[newpi] index: %d, value: %d", (index+spos+(plabel-1)*(ndims/3)+r_label-1), newpi);
			oldpi = ((int*)ARR_DATA_PTR(factors))[index+spos+(plabel-1)*nlabels+olabel-1];
//elog(DEBUG1, "[oldpi] index: %d, value: %d", (index+spos+(plabel-1)*(ndims/3)+olabel-1), oldpi);
		}
elog(DEBUG1, "newpi, oldpi, r_alpha: %d, %d, %f", newpi, oldpi,r_alpha);

		if (r_start_pos < (ndims/3-1)) {
			nitems = ((int*)ARR_DATA_PTR(factor_lens))[r_start_pos+1];
			spos = nitems % (nlabels*nlabels);
			plabel = ((int*)ARR_DATA_PTR(cur))[(ndims/3)*2+r_start_pos+1];
elog(DEBUG1, "postlabel2: %d", plabel);
			int index2 = ((int*)ARR_DATA_PTR(factor_lens))[r_start_pos];
			newpi = newpi + ((int*)ARR_DATA_PTR(factors))[index+index2+spos+(r_label-1)*nlabels+plabel-1];
			oldpi = oldpi + ((int*)ARR_DATA_PTR(factors))[index+index2+spos+(olabel-1)*nlabels+plabel-1];
		}

		r_alpha = exp(((float)(newpi-oldpi))/1000.0);
//		if (r_alpha>1.0)	r_alpha = 1.0;


		input = GetAttributeByName(v1, "alpha", &isnull);
		if (isnull)  
			PG_RETURN_NULL();
		v1_alpha = DatumGetFloat4(input);
//elog(DEBUG1, "old v1_alpha: %f", v1_alpha); 
		inputref = GetAttributeByRef(v1, "alpha", &isnull);
		*((float4*)DatumGetPointer(inputref)) = r_alpha;

//		input = GetAttributeByName(v1, "alpha", &isnull);
//		if (isnull)  
//			PG_RETURN_NULL();
//		v1_alpha = DatumGetFloat4(input);
//elog(DEBUG1, "new v1_alpha: %f", v1_alpha); 

elog(DEBUG1, "newpi, oldpi, r_alpha: %d, %d, %f", newpi, oldpi, r_alpha);
	}
 	//r = (((double)rand())/(((double)RAND_MAX)+((double)1))); 
 	r = drand48();
elog(DEBUG1, "random: %f", r);

/* for computing distribution */
	if (r < r_alpha) {
if(r_label != olabel)
elog(DEBUG1, "accept the proposal values", r);
 
/* for computing MAX */
//	if (r_alpha > 1) {

	// update the cur
	((int*)ARR_DATA_PTR(cur))[(ndims/3)*2+r_start_pos]=r_label;
	} else {

elog(DEBUG1, "fill in dummy values", r);
	// fill in dummy values 
	r_doc_id = -1;
	inputref = GetAttributeByRef(v1, "doc_id", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_doc_id;
	r_start_pos = -1;
	inputref = GetAttributeByRef(v1, "start_pos", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_start_pos;
	r_label = -1;
	inputref = GetAttributeByRef(v1, "label", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_label;

	}

	//factors and factor_lens stay the same

	PG_RETURN_HEAPTUPLEHEADER(v1);
}

typedef struct query_io
{
	int		doc_id;
	int		start_pos;
	int		label;
	int		*cur;
} query_io;

/******************************************************
 * SimpleQuery1
 *
 * create function SimpleQuery1(query_io, query_io) returns getalpha_io as '/home/daisyw/p84/mingw-postgresql-8.4dev/src/bayesstore/ie_lccrf_mcmcmh', 'SimpleQuery1' language c called on null input;
 *
 * ****************************************************/

PG_FUNCTION_INFO_V1(SimpleQuery1);

Datum
SimpleQuery1(PG_FUNCTION_ARGS)
{
	bool 		isnull;
	Datum		input;
	Datum		inputref;
	bool		isupdate= false;	// is initial running state

	int32		v1_doc_id, v1_start_pos, v1_label;

	int32		v2_doc_id, v2_start_pos, v2_label;

	ArrayType	*cur;

	int32		r_doc_id, r_start_pos, r_label;
	ArrayType	*next;

	TupleDesc	tupdesc;
	HeapTuple	tup;
	Datum		values[4];
	bool		nulls[4] = {0,0,0,0};
	Datum		result;

	if (PG_ARGISNULL(1))
		abort();

	// get new aggregate input
        HeapTupleHeader v2 = PG_GETARG_HEAPTUPLEHEADER(1);
	input = GetAttributeByName(v2, "doc_id", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_doc_id = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_start_pos = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "label", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_label = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "cur", &isnull);
	if (isnull)
		cur = NULL;
	else {
		isupdate = true;
      // TODO: if cur != input, then dealing with detoasted value, which should be freed. eg. PG_FREE_IF_COPY(cur,input) --- cur is freed if it is a detoasted copy of input (ie, if cur!=input)
		cur = DatumGetArrayTypeP(input);
	}

	if (PG_ARGISNULL(0) || isupdate==true) {
//elog(DEBUG1, "***Update State***");

		if (cur == NULL)	
			PG_RETURN_NULL();

		if (minidx_array(cur,1) != -1 && maxidx_array(cur,2) != -1 && minidx_array(cur,1) < maxidx_array(cur,2)) {
			// fill in dummy values 
			r_doc_id = -1;
			values[0] = Int32GetDatum(r_doc_id); 
			r_start_pos = -1;
			values[1] = Int32GetDatum(r_start_pos);
			r_label = -1;
			values[2] = Int32GetDatum(r_label);
		} else {	
			// construct a new composite type 
			// as result running state
			r_doc_id = v2_doc_id;
			values[0] = Int32GetDatum(r_doc_id); 
			r_start_pos = v2_start_pos;
			values[1] = Int32GetDatum(r_start_pos);
			r_label= v2_label;
			values[2] = Int32GetDatum(r_label);
		}

      // TODO: do we need to free this copy once tuple has been formed ????
		next = DatumGetArrayTypePCopy(input); // NOTE: input must be "cur" ... see above GetAttributeByName
		((int*)ARR_DATA_PTR(next))[r_start_pos]=r_label;
		values[3] = PointerGetDatum(next); 

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE){
			ereport(ERROR,
       	             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
       	              errmsg("function returning record called in context that cannot accept type record")));
			tupdesc = BlessTupleDesc(tupdesc);
		}

		tup = heap_form_tuple(tupdesc, values, nulls);

		result = HeapTupleGetDatum(tup);

   		PG_RETURN_DATUM(result);
	}

	// get running aggregate state 
       HeapTupleHeader v1 = PG_GETARG_HEAPTUPLEHEADER(0);
 //       HeapTupleHeader v1 = DatumGetHeapTupleHeader(PG_GETARG_DATUM(0));
	input = GetAttributeByName(v1, "cur", &isnull);
	if (isnull)  
		PG_RETURN_NULL();
	else {
		elog(DEBUG1, "cur assigned to v1.cur");
		cur = DatumGetArrayTypeP(input);
	}
elog(DEBUG1, "cur %p, %p state v1.cur[%d] is %d", input, cur, v2_start_pos, ((int*)ARR_DATA_PTR(cur))[v2_start_pos]);

	if (cur == NULL )
		PG_RETURN_NULL();

	if (minidx_array(cur,1) != -1 && maxidx_array(cur,2) != -1 && minidx_array(cur,1) < maxidx_array(cur,2)) {
elog(DEBUG1, "fill in dummy values");
		// fill in dummy values 
		r_doc_id = -1;
		inputref = GetAttributeByRef(v1, "doc_id", &isnull);
		*((int32*)DatumGetPointer(inputref)) = r_doc_id;
		r_start_pos = -1;
		inputref = GetAttributeByRef(v1, "start_pos", &isnull);
		*((int32*)DatumGetPointer(inputref)) = r_start_pos;
		r_label = -1;
		inputref = GetAttributeByRef(v1, "label", &isnull);
		*((int32*)DatumGetPointer(inputref)) = r_label;
	} else {
		// fill in values of the new running state
		r_doc_id = v2_doc_id;
		inputref = GetAttributeByRef(v1, "doc_id", &isnull);
		*((int32*)DatumGetPointer(inputref)) = r_doc_id;
		r_start_pos = v2_start_pos;
		inputref = GetAttributeByRef(v1, "start_pos", &isnull);
		*((int32*)DatumGetPointer(inputref)) = r_start_pos;
		inputref = GetAttributeByRef(v1, "label", &isnull);
		r_label = v2_label;
		*((int32*)DatumGetPointer(inputref)) = r_label;
	}

	input = GetAttributeByName(v1, "cur", &isnull);
	cur = DatumGetArrayTypeP(input);
elog(DEBUG1, "cur %p, %p state v1.cur[%d] is %d", input, cur, v2_start_pos, ((int*)ARR_DATA_PTR(cur))[v2_start_pos]);

	input = GetAttributeByName(v1, "cur", &isnull);
	cur = DatumGetArrayTypeP(input);
elog(DEBUG1, "cur %p, %p state v1.cur[%d] is %d", input, cur, v2_start_pos, ((int*)ARR_DATA_PTR(cur))[v2_start_pos]);

	((int*)ARR_DATA_PTR(cur))[v2_start_pos]=v2_label;
elog(DEBUG1, "update %p, %p state v1.cur[%d] to %d", input, cur, v2_start_pos, ((int*)ARR_DATA_PTR(cur))[v2_start_pos]);

	input = GetAttributeByName(v1, "cur", &isnull);
	cur = DatumGetArrayTypeP(input);
elog(DEBUG1, "cur %p, %p state v1.cur[%d] is %d", input, cur, v2_start_pos, ((int*)ARR_DATA_PTR(cur))[v2_start_pos]);

	PG_RETURN_HEAPTUPLEHEADER(v1);
}

/******************************************************
 * SimpleQuery1_getalpha
 *
 * create function SimpleQuery1_getalpha(getalpha_io, getalpha_io) returns getalpha_io as '/home/daisyw/p84/mingw-postgresql-8.4dev/src/bayesstore/ie_lccrf_mcmcmh', 'SimpleQuery1_getalpha' language c called on null input;
 *
 * ****************************************************/

/* TODO: to integrate with getalpha */
/* NOTE: the third param is of different dimensions */ 

PG_FUNCTION_INFO_V1(SimpleQuery1_getalpha);

Datum
SimpleQuery1_getalpha(PG_FUNCTION_ARGS)
{
	bool 		isnull;
	Datum		input;
	Datum		inputref;
	bool		isupdate= false;	// is initial running state

	int32		v1_doc_id, v1_start_pos, v1_label;
	float4		v1_alpha;

	int32		v2_doc_id, v2_start_pos, v2_label;
	float4		v2_alpha;

	ArrayType	*cur;
	ArrayType	*factors;
	ArrayType	*factor_lens;

	int32		r_doc_id, r_start_pos, r_label;
	float4		r_alpha;
	ArrayType	*next;
	ArrayType	*r_factors;
	ArrayType	*r_factor_lens;

	TupleDesc	tupdesc;
	HeapTuple	tup;
	Datum		values[7];
	bool		nulls[7] = {0,0,0,0,0,0,0};
	Datum		result;

	int32		olabel;
	int32		ndims;
	int		i;
	double		r;

elog(DEBUG1, "***IN new GETALPHA***");

	if (PG_ARGISNULL(1))
		abort();

	// get new aggregate input
        HeapTupleHeader v2 = PG_GETARG_HEAPTUPLEHEADER(1);
	input = GetAttributeByName(v2, "doc_id", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_doc_id = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_start_pos = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "cur", &isnull);
	if (isnull)
		cur = NULL;
	else {
		isupdate = true;
		cur = DatumGetArrayTypeP(input);
		ndims = ARR_DIMS(cur)[0];
	}
	input = GetAttributeByName(v2, "factors", &isnull);
	if (isnull)
		factors = NULL;
	else 
		factors = DatumGetArrayTypeP(input);
	input = GetAttributeByName(v2, "factor_lens", &isnull);
	if (isnull)
		factor_lens = NULL;
	else 
		factor_lens = DatumGetArrayTypeP(input);

	if (PG_ARGISNULL(0) || isupdate==true) {
//elog(DEBUG1, "***Update State***");

		if (cur == NULL || factors == NULL || factor_lens == NULL)		
			PG_RETURN_NULL();

		// construct a new composite type 
		// as result running state
		r_doc_id = v2_doc_id;
		values[0] = Int32GetDatum(r_doc_id); 
		r_start_pos = v2_start_pos;
		values[1] = Int32GetDatum(r_start_pos);

		next = DatumGetArrayTypePCopy(cur);
		r_factors = DatumGetArrayTypePCopy(factors);
		r_factor_lens= DatumGetArrayTypePCopy(factor_lens);

		values[4] = PointerGetDatum(next); 
		values[5] = PointerGetDatum(r_factors);
		values[6] = PointerGetDatum(r_factor_lens);

	// compute label that satisfy SimpleQuery1
	{
	// title tokens (1) has to be after author tokens (2)
//	int before = 2;
//	int after = 1;
	// title tokens (1) has to be in front of author tokens (2)
	int before = 1;
	int after = 2;

		if (minidx_array(cur,after)!=-1 && v2_start_pos > minidx_array(cur,after)) {
                        r_label = before;
                        while (r_label == before || r_label > 10)
                                r_label = ((int)(drand48() * 10)) + 1;
                } else if (maxidx_array(cur,before)!=-1 && v2_start_pos < maxidx_array(cur,before)) {
                        r_label = after;
                        while (r_label == after || r_label > 10)
                                r_label = ((int)(drand48() * 10)) + 1;
                } else {
                        while (r_label > 10)
                                r_label = ((int)(drand48() * 10)) + 1;
                }

		values[2] = Int32GetDatum(r_label);
	}
elog(DEBUG1, "[new state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, r_label);


	// compute alpha 
	{
		olabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos];
	      	int nitems = ((int*)ARR_DATA_PTR(r_factor_lens))[v2_start_pos];	
		int nlabels = sqrt(nitems);
		int spos = nitems % (nlabels*nlabels);
		int index = 0;
		int plabel;
		int newpi, oldpi;
elog(DEBUG1, "[old state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, olabel);

		if (v2_start_pos == 0) {
			newpi = ((int*)ARR_DATA_PTR(r_factors))[index+r_label-1];
//elog(DEBUG1, "[newpi] index: %d, value: %d", (index+r_label-1), newpi);
			oldpi = ((int*)ARR_DATA_PTR(r_factors))[index+olabel-1];
//elog(DEBUG1, "[oldpi] index: %d, value: %d", (index+olabel-1), oldpi);
		} else {
			plabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos-1];
elog(DEBUG1, "prevlabel: %d", plabel);
			for (i=0; i<v2_start_pos; i++)
				index += ((int*)ARR_DATA_PTR(r_factor_lens))[i]; 
			newpi = ((int*)ARR_DATA_PTR(r_factors))[index+spos+(plabel-1)*nlabels+r_label-1];
			oldpi = ((int*)ARR_DATA_PTR(r_factors))[index+spos+(plabel-1)*nlabels+olabel-1];
		}

		if (v2_start_pos < (ndims-1)) {
			nitems = ((int*)ARR_DATA_PTR(r_factor_lens))[v2_start_pos+1];
			spos = nitems % (nlabels*nlabels);
			plabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos+1];
elog(DEBUG1, "postlabel: %d", plabel);
			int index2 = ((int*)ARR_DATA_PTR(r_factor_lens))[v2_start_pos];
			newpi = newpi + ((int*)ARR_DATA_PTR(r_factors))[index+index2+spos+(r_label-1)*nlabels+plabel-1];
			oldpi = oldpi + ((int*)ARR_DATA_PTR(r_factors))[index+index2+spos+(olabel-1)*nlabels+plabel-1];
		}

		r_alpha = exp(((float)(newpi-oldpi))/1000.0);
//		if (r_alpha>1.0)	r_alpha = 1.0;
		values[3] = Float4GetDatum(r_alpha);
elog(DEBUG1, "newpi, oldpi, r_alpha: %d, %d, %f", newpi, oldpi,r_alpha);
	}

 	//r = (((double)rand())/(((double)RAND_MAX)+((double)1))); 
 	r = drand48();
elog(DEBUG1, "random: %f", r);

/* for computing distribution */
	if (r < r_alpha) {

/* for computing MAX */
//	if (r_alpha > 1) {

	// update state
		if(r_label != olabel) {
elog(DEBUG1, "accept the proposal values", r);
			((int*)ARR_DATA_PTR(next))[v2_start_pos]=r_label;
			// update the start_pos in running state, which is the number of consecutive same samples
			r_start_pos = 1;
			values[1] = Int32GetDatum(r_start_pos);
		} else {
			// update the start_pos 
			r_start_pos = 2;
			values[1] = Int32GetDatum(r_start_pos);
		}

	} else {
elog(DEBUG1, "fill in dummy values", r);

	// fill in dummy values 
	r_doc_id = -1;
	values[0] = Int32GetDatum(r_doc_id); 
	r_start_pos = 2;
	values[1] = Int32GetDatum(r_start_pos);
	r_label = -1;
	values[2] = Int32GetDatum(r_label);
	}

		// build a tuple descriptor for our result type 
		if (/*((counter++)%10000==0 || tupdesc == NULL) &&*/ get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE){
			ereport(ERROR,
       	             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
       	              errmsg("function returning record called in context that cannot accept type record")));
			tupdesc = BlessTupleDesc(tupdesc);
		}

//		for (i=4; i<(tupdesc->natts); i++) {
//			nulls[i] = 1;
//elog(DEBUG1, "nulls[%d]: %d", i, nulls[i]);
//		}
//elog(DEBUG1, "tupdesc->natts: %d", tupdesc->natts);

		tup = heap_form_tuple(tupdesc, values, nulls);

		result = HeapTupleGetDatum(tup);

   		PG_RETURN_DATUM(result);
	}

	// get running aggregate state 
        HeapTupleHeader v1 = PG_GETARG_HEAPTUPLEHEADER(0);
	// doc_id, start_pos, label is not used
	input = GetAttributeByName(v1, "doc_id", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_doc_id = DatumGetInt32(input); 
	input = GetAttributeByName(v1, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_start_pos = DatumGetInt32(input); 
	input = GetAttributeByName(v1, "label", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_label = DatumGetInt32(input); 
	input = GetAttributeByName(v1, "cur", &isnull);
	if (isnull)  
		PG_RETURN_NULL();
	else {
		cur = DatumGetArrayTypeP(input);
		ndims = ARR_DIMS(cur)[0];
	}
	input = GetAttributeByName(v1, "factors", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	else 
		factors = DatumGetArrayTypeP(input);
	input = GetAttributeByName(v1, "factor_lens", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	else 
		factor_lens = DatumGetArrayTypeP(input);

	// fill in values of the new running state (v1)
	// Note: different from the data (v2) 
	// doc_id is encoding if the proposal is rejected (-1) or the doc_id of the proposal 
	// start_pos is encoding how many samples has been the same (either proposals being rejected, or the proposals make no changes)
	r_doc_id = v2_doc_id;
	inputref = GetAttributeByRef(v1, "doc_id", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_doc_id;
	
	// compute label that satisfy SimpleQuery1
	{
	// title tokens (1) has to be after author tokens (2)
//	int before = 2;
//	int after = 1;
	// title tokens (1) has to be in front of author tokens (2)
	int before = 1;
	int after = 2;

 		if (minidx_array(cur,after)!=-1 && v2_start_pos > minidx_array(cur,after)) {
                	r_label = before;
                	while (r_label == before || r_label > 10)
                       		r_label = ((int)(drand48() * 10)) + 1; 
        	} else if (maxidx_array(cur,before)!=-1 && v2_start_pos < maxidx_array(cur,before)) {
                	r_label = after;
                	while (r_label == after || r_label > 10)
                        	r_label = ((int)(drand48() * 10)) + 1; 
        	} else {
                	while (r_label > 10)
                        	r_label = ((int)(drand48() * 10)) + 1;
        	}
elog(DEBUG1, "[new state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, r_label);

		inputref = GetAttributeByRef(v1, "label", &isnull);
		*((int32*)DatumGetPointer(inputref)) = r_label;
	}

	// compute new alpha
//elog(DEBUG1, "***compute new alpha***");
	{
		olabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos];
	      	int nitems = ((int*)ARR_DATA_PTR(factor_lens))[v2_start_pos];	
		int nlabels = sqrt(nitems);
		int spos = nitems % (nlabels*nlabels);
		int index = 0;
		int plabel;
		int newpi, oldpi;
elog(DEBUG1, "[old state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, olabel);

		if (v2_start_pos == 0) {
			newpi = ((int*)ARR_DATA_PTR(factors))[index+r_label-1];
//elog(DEBUG1, "[newpi] index: %d, value: %d", (index+r_label-1), newpi);
			oldpi = ((int*)ARR_DATA_PTR(factors))[index+olabel-1];
//elog(DEBUG1, "[oldpi] index: %d, value: %d", (index+olabel-1), oldpi);
		} else {
			plabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos-1];
elog(DEBUG1, "prevlabel: %d", plabel);
			for (i=0; i<v2_start_pos; i++)
				index += ((int*)ARR_DATA_PTR(factor_lens))[i]; 
			newpi = ((int*)ARR_DATA_PTR(factors))[index+spos+(plabel-1)*nlabels+r_label-1];
			oldpi = ((int*)ARR_DATA_PTR(factors))[index+spos+(plabel-1)*nlabels+olabel-1];
		}

		if (v2_start_pos < (ndims-1)) {
			nitems = ((int*)ARR_DATA_PTR(factor_lens))[v2_start_pos+1];
			spos = nitems % (nlabels*nlabels);
			plabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos+1];
elog(DEBUG1, "postlabel: %d", plabel);
			int index2 = ((int*)ARR_DATA_PTR(factor_lens))[v2_start_pos];
			newpi = newpi + ((int*)ARR_DATA_PTR(factors))[index+index2+spos+(r_label-1)*nlabels+plabel-1];
			oldpi = oldpi + ((int*)ARR_DATA_PTR(factors))[index+index2+spos+(olabel-1)*nlabels+plabel-1];
		}

		r_alpha = exp(((float)(newpi-oldpi))/1000.0);
//		if (r_alpha>1.0)	r_alpha = 1.0;


		input = GetAttributeByName(v1, "alpha", &isnull);
		if (isnull)  
			PG_RETURN_NULL();
		v1_alpha = DatumGetFloat4(input);
//elog(DEBUG1, "old v1_alpha: %f", v1_alpha); 
		inputref = GetAttributeByRef(v1, "alpha", &isnull);
		*((float4*)DatumGetPointer(inputref)) = r_alpha;

//		input = GetAttributeByName(v1, "alpha", &isnull);
//		if (isnull)  
//			PG_RETURN_NULL();
//		v1_alpha = DatumGetFloat4(input);
//elog(DEBUG1, "new v1_alpha: %f", v1_alpha); 

elog(DEBUG1, "newpi, oldpi, r_alpha: %d, %d, %f", newpi, oldpi, r_alpha);
	}
 	//r = (((double)rand())/(((double)RAND_MAX)+((double)1))); 
 	r = drand48();
elog(DEBUG1, "random: %f", r);

/* for computing distribution -- normal
 * temperature = 1
 */
	if (r < r_alpha) {
 
/* for computing MAX -- hill-climbinb
 * temperature = 0
 */
//	if (r_alpha > 1) {

		if(r_label != olabel) {
elog(DEBUG1, "accept the proposal values", r);
			// update the cur
			((int*)ARR_DATA_PTR(cur))[v2_start_pos]=r_label;

			// update the start_pos in running state, which is the number of consecutive same samples
			r_start_pos = 1;
			inputref = GetAttributeByRef(v1, "start_pos", &isnull);
			*((int32*)DatumGetPointer(inputref)) = r_start_pos;
		} else {
			// update the start_pos 
			r_start_pos = v1_start_pos + 1;
			//TODO: hardcoding the shoppting condition
			//if (r_start_pos > 100)
			//	PG_RETURN_NULL();
				
			inputref = GetAttributeByRef(v1, "start_pos", &isnull);
			*((int32*)DatumGetPointer(inputref)) = r_start_pos;

		}
	} else {

elog(DEBUG1, "fill in dummy values", r);
	// fill in dummy values 
	r_doc_id = -1;
	inputref = GetAttributeByRef(v1, "doc_id", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_doc_id;
	r_start_pos = v1_start_pos + 1;
	inputref = GetAttributeByRef(v1, "start_pos", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_start_pos;
	r_label = -1;
	inputref = GetAttributeByRef(v1, "label", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_label;

	}

	//factors and factor_lens stay the same

	PG_RETURN_HEAPTUPLEHEADER(v1);
}

/******************************************************
 * Gibbs_getalpha
 *
 * create function Gibbs_getalpha(getalpha_io, getalpha_io) returns getalpha_io as '/home/daisyw/p84/mingw-postgresql-8.4dev/src/bayesstore/ie_lccrf_mcmcmh', 'Gibbs_getalpha' language c called on null input;
 *
 * ****************************************************/

PG_FUNCTION_INFO_V1(Gibbs_getalpha);

Datum
Gibbs_getalpha(PG_FUNCTION_ARGS)
{
	bool 		isnull;
	Datum		input;
	Datum		inputref;
	bool		isupdate= false;	// is initial running state

	int32		v1_doc_id, v1_start_pos, v1_label;
	float4		v1_alpha;

	int32		v2_doc_id, v2_start_pos, v2_label;
	float4		v2_alpha;

	ArrayType	*cur;
	ArrayType	*factors;
	ArrayType	*factor_lens;

	int32		r_doc_id, r_start_pos, r_label;
	float4		r_alpha;
	ArrayType	*next;
	ArrayType	*r_factors;
	ArrayType	*r_factor_lens;

	TupleDesc	tupdesc;
	HeapTuple	tup;
	Datum		values[7];
	bool		nulls[7] = {0,0,0,0,0,0,0};
	Datum		result;

	int32		olabel;
	int32		ndims;
	int		i;
	double		r;

elog(DEBUG1, "***IN new GETALPHA***");

	if (PG_ARGISNULL(1))
		abort();

	// get new aggregate input
        HeapTupleHeader v2 = PG_GETARG_HEAPTUPLEHEADER(1);
	input = GetAttributeByName(v2, "doc_id", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_doc_id = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_start_pos = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "cur", &isnull);
	if (isnull)
		cur = NULL;
	else {
		isupdate = true;
		cur = DatumGetArrayTypeP(input);
		ndims = ARR_DIMS(cur)[0];
	}
	input = GetAttributeByName(v2, "factors", &isnull);
	if (isnull)
		factors = NULL;
	else 
		factors = DatumGetArrayTypeP(input);
	input = GetAttributeByName(v2, "factor_lens", &isnull);
	if (isnull)
		factor_lens = NULL;
	else 
		factor_lens = DatumGetArrayTypeP(input);

	if (PG_ARGISNULL(0) || isupdate==true) {
//elog(DEBUG1, "***Update State***");

		if (cur == NULL || factors == NULL || factor_lens == NULL)		
			PG_RETURN_NULL();

		// construct a new composite type 
		// as result running state
		r_doc_id = v2_doc_id;
		values[0] = Int32GetDatum(r_doc_id); 
		r_start_pos = v2_start_pos;
		values[1] = Int32GetDatum(r_start_pos);

		next = DatumGetArrayTypePCopy(cur);
		r_factors = DatumGetArrayTypePCopy(factors);
		r_factor_lens= DatumGetArrayTypePCopy(factor_lens);

		values[4] = PointerGetDatum(next); 
		values[5] = PointerGetDatum(r_factors);
		values[6] = PointerGetDatum(r_factor_lens);

	// compute label according to local probability distribution 
	{
	    int nitems = ((int*)ARR_DATA_PTR(r_factor_lens))[v2_start_pos];	
	    int nlabels = sqrt(nitems);
	    int spos = nitems % (nlabels*nlabels);
	    int index = 0;
	    int label, plabel;
	    float pis[nlabels];
	    int pi;

	    for (label=1; label<=nlabels; label++) {
	        nitems = ((int*)ARR_DATA_PTR(r_factor_lens))[v2_start_pos];	
	        spos = nitems % (nlabels*nlabels);
	        index = 0;
		if (v2_start_pos == 0) {
			pi = ((int*)ARR_DATA_PTR(r_factors))[index+label-1];
		} else {
			plabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos-1];
			if (plabel = nlabels)
				plabel = 2;	
if (label ==1) elog(DEBUG1, "prevlabel: %d", plabel);
			for (i=0; i<v2_start_pos; i++)
				index += ((int*)ARR_DATA_PTR(r_factor_lens))[i]; 
			pi = ((int*)ARR_DATA_PTR(r_factors))[index+spos+(plabel-1)*nlabels+label-1];
		}
elog(DEBUG1, "label %d, pi %d, %f", label, pi, pis[label-1]);

		if (v2_start_pos < (ndims-1)) {
			nitems = ((int*)ARR_DATA_PTR(r_factor_lens))[v2_start_pos+1];
			spos = nitems % (nlabels*nlabels);
			plabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos+1];
			if (plabel = nlabels)
				plabel = 2;	
if (label==1) elog(DEBUG1, "postlabel: %d", plabel);
			int index2 = ((int*)ARR_DATA_PTR(r_factor_lens))[v2_start_pos];
			pi = pi + ((int*)ARR_DATA_PTR(r_factors))[index+index2+spos+(label-1)*nlabels+plabel-1];
		}

		if (label == 1) pis[label-1] = exp((float)(pi)/1000);
		else pis[label-1] = pis[label-2]+exp((float)(pi)/1000);
//		pis[label-1] = pi;

elog(DEBUG1, "label %d, pi %d, %f", label, pi, pis[label-1]);
	    }
		
 	    r = drand48();
elog(DEBUG1, "random: %f", r);
 	    r = r*pis[nlabels-1];
elog(DEBUG1, "random: %f", r);

	    r_label=nlabels;
	    for (label=1; label<=nlabels; label++) {
		if (pis[label-1]>=r) {
			r_label = label;	
			break;
		}
            }
/*
	    float max = pis[0];
	    r_label = 1;
            for (label=2; label<=nlabels; label++) {
		float v = pis[label-1];
                if (v>max){
                        r_label = label;      
			max = v;	
		}
            }
*/
	    values[2] = Int32GetDatum(r_label);
       	    values[3] = Float4GetDatum(1.0);
elog(DEBUG1, "[new state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, r_label);
	}

	((int*)ARR_DATA_PTR(next))[v2_start_pos]=r_label;

		// build a tuple descriptor for our result type 
		if ( get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE){
			ereport(ERROR,
       	             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
       	              errmsg("function returning record called in context that cannot accept type record")));
			tupdesc = BlessTupleDesc(tupdesc);
		}

		tup = heap_form_tuple(tupdesc, values, nulls);

		result = HeapTupleGetDatum(tup);

   		PG_RETURN_DATUM(result);
	}

	// get running aggregate state 
        HeapTupleHeader v1 = PG_GETARG_HEAPTUPLEHEADER(0);
	// doc_id, start_pos, label is not used
	input = GetAttributeByName(v1, "doc_id", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_doc_id = DatumGetInt32(input); 
	input = GetAttributeByName(v1, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_start_pos = DatumGetInt32(input); 
	input = GetAttributeByName(v1, "label", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_label = DatumGetInt32(input); 
	input = GetAttributeByName(v1, "cur", &isnull);
	if (isnull)  
		PG_RETURN_NULL();
	else {
		cur = DatumGetArrayTypeP(input);
		ndims = ARR_DIMS(cur)[0];
	}
	input = GetAttributeByName(v1, "factors", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	else 
		factors = DatumGetArrayTypeP(input);
	input = GetAttributeByName(v1, "factor_lens", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	else 
		factor_lens = DatumGetArrayTypeP(input);

	// fill in values of the new running state (v1)
	r_doc_id = v2_doc_id;
	inputref = GetAttributeByRef(v1, "doc_id", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_doc_id;
	r_start_pos = v2_start_pos;
	inputref = GetAttributeByRef(v1, "start_pos", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_start_pos;
	
	// compute label according to local probability distribution 
	{
	      	int nitems = ((int*)ARR_DATA_PTR(factor_lens))[v2_start_pos];	
		int nlabels = sqrt(nitems);
		int spos = nitems % (nlabels*nlabels);
		int index = 0;
		int label, plabel;
		int pi;
		float pis[nlabels];

	    for (label=1; label <= nlabels; label++) {
	        nitems = ((int*)ARR_DATA_PTR(factor_lens))[v2_start_pos];	
	        spos = nitems % (nlabels*nlabels);
	        index = 0;
		if (v2_start_pos == 0) {
			pi = ((int*)ARR_DATA_PTR(factors))[index+label-1];
		} else {
			plabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos-1];
			if (plabel = nlabels)
				plabel = 2;	
if (label==1) elog(DEBUG1, "prevlabel: %d, v2_start_pos: %d", plabel, v2_start_pos);
			for (i=0; i<v2_start_pos; i++)
				index += ((int*)ARR_DATA_PTR(factor_lens))[i]; 
			pi = ((int*)ARR_DATA_PTR(factors))[index+spos+(plabel-1)*nlabels+label-1];
		}
elog(DEBUG1, "label %d, pi %d, %f", label, pi, pis[label-1]);

		if (v2_start_pos < (ndims-1)) {
			nitems = ((int*)ARR_DATA_PTR(factor_lens))[v2_start_pos+1];
			spos = nitems % (nlabels*nlabels);
			plabel = ((int*)ARR_DATA_PTR(cur))[v2_start_pos+1];
			if (plabel = nlabels)
				plabel = 2;	
if (label==1) elog(DEBUG1, "postlabel: %d", plabel);
			int index2 = ((int*)ARR_DATA_PTR(factor_lens))[v2_start_pos];
			pi = pi + ((int*)ARR_DATA_PTR(factors))[index+index2+spos+(label-1)*nlabels+plabel-1];
		}
 
                if (label == 1) pis[label-1] = exp((float)(pi)/1000);
                else pis[label-1] = pis[label-2]+exp((float)(pi)/1000);
//		pis[label-1] = pi;
elog(DEBUG1, "label %d, pi %d, %f", label, pi, pis[label-1]);
            }

            r = drand48();
elog(DEBUG1, "random: %f", r);
            r = r*pis[nlabels-1];
elog(DEBUG1, "random: %f", r);

	    r_label = nlabels;
            for (label=1; label<=nlabels; label++) {
                if (pis[label-1]>=r){
                        r_label = label;      
			break;
		}
            }
/*
	    float max = pis[0];
	    r_label = 1;
            for (label=2; label<=nlabels; label++) {
		float v = pis[label-1];
                if (v>max){
                        r_label = label;      
			max = v;	
		}
            }
*/
   	    inputref = GetAttributeByRef(v1, "label", &isnull);
	    *((int32*)DatumGetPointer(inputref)) = r_label;	
elog(DEBUG1, "[new state] doc_id: %d, start_pos: %d, label: %d", v2_doc_id, v2_start_pos, r_label);
	}
	
	((int*)ARR_DATA_PTR(cur))[v2_start_pos]=r_label;

	//factors and factor_lens stay the same

	PG_RETURN_HEAPTUPLEHEADER(v1);
}


/******************************************************
 * biased_samples: generate samples within the range with a biased probability (\beta) to remain as the previous sample, and a random sample otherwise 
 *
 * create function biased_samples(integer, integer, integer, real) returns setof integer as '/home/daisyw/p84/mingw-postgresql-8.4dev/src/bayesstore/ie_lccrf_mcmcmh', 'biased_samples' language c strict;
 *
 * ****************************************************/

typedef struct
{
	int32		current;
	int32		current_num;
        int32           start;
        int32           finish;
	int32		num_samples;
        float4		beta;
} biased_samples_fctx;

PG_FUNCTION_INFO_V1(biased_samples);

Datum
biased_samples(PG_FUNCTION_ARGS)
{
        FuncCallContext *funcctx;
        int32           result;
	biased_samples_fctx	*fctx;
        MemoryContext oldcontext;

        int32           start = PG_GETARG_INT32(0);
        int32           finish = PG_GETARG_INT32(1);
        int32           num_samples = PG_GETARG_INT32(2);
        float4		beta = PG_GETARG_FLOAT4(3);

        if (beta > 1 || beta<0) 
       		ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("beta has to be within [0,1]")));

 /* stuff done only on the first call of the function */
        if (SRF_IS_FIRSTCALL())
        {
                /* create a function context for cross-call persistence */
                funcctx = SRF_FIRSTCALL_INIT();
                /*
 *                  * switch to memory context appropriate for multiple function calls
 *                                   */
                oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

                /* allocate memory for user context */
                fctx = (biased_samples_fctx *) palloc(sizeof(biased_samples_fctx));

                /*
 *                  * Use fctx to keep state from call to call. Seed current with the
 *                                   * original start value
 *                                                    */
                //fctx->current = ((int)((((double)rand())/(((double)RAND_MAX)+((double)1))) * (1+finish-start)) + start); 
                fctx->current = ((int)(drand48() * (1+finish-start)) + start); 
                fctx->current_num = 1;
                fctx->start = start;
                fctx->finish = finish;
                fctx->num_samples = num_samples;
                fctx->beta = beta;

                funcctx->user_fctx = fctx;
                MemoryContextSwitchTo(oldcontext);
        }

        /* stuff done on every call of the function */
        funcctx = SRF_PERCALL_SETUP();

/*
 *          * get the saved state and use current as the result for this iteration
 *                   */
        fctx = funcctx->user_fctx;
        result = fctx->current;

        if (fctx->current_num <= fctx->num_samples)
        {
                /* update current in preparation for next iteration */
		double prob;

                //prob = (((double)rand())/((double)RAND_MAX)); 
                prob = (((double)lrand48())/((double)RAND_MAX));
		
		/* only (1-beta) probability pick another random number */
		if (prob > beta) {
                    //fctx->current = ((int)((((double)rand())/(((double)RAND_MAX)+((double)1))) * (1+finish-start)) + start); 
                    fctx->current = ((int)(drand48() * (1+finish-start)) + start); 
		} 

		fctx->current_num = fctx->current_num + 1;
			
                /* do when there is more left to send */
                SRF_RETURN_NEXT(funcctx, Int32GetDatum(result));
        }
        else
                /* do when there is no more left */
                SRF_RETURN_DONE(funcctx);
}

/******************************************************
 * SimpleQuery1_getlabel 
 *
 * create function SimpleQuery1_getlabel(getlabel_io, getlabel_io) returns getlabel_io as '/home/daisyw/p84/mingw-postgresql-8.4dev/src/bayesstore/ie_lccrf_mcmcmh', 'SimpleQuery1_getlabel' language c called on null input;
 *
 * ****************************************************/

typedef struct getlabel_io
{
	int		start_pos;
	int		label;
	int		*cur;
} getlabel_io;

PG_FUNCTION_INFO_V1(SimpleQuery1_getlabel);

Datum
SimpleQuery1_getlabel(PG_FUNCTION_ARGS)
{
	bool		isnull;
 	Datum		input;
	Datum		inputref;
	bool 		isupdate = false;

	int32		v1_start_pos;
	int32		v1_label;
	ArrayType	*v1_cur;

	int32		v2_start_pos;
	int32		v2_label;
	ArrayType	*v2_cur;

	int32		r_label;
	int32		r_start_pos;
	ArrayType	*r_cur;

	TupleDesc	tupdesc;
	HeapTuple	tup;
	Datum		values[3];
	bool		nulls[3] = {0,0,0};
	Datum		result;

	int32		ndims;

elog(DEBUG1, "***IN SimpleQuery1_getlabel***");

	if (PG_ARGISNULL(1))
		abort();

	// get new aggregate input
	HeapTupleHeader v2 = PG_GETARG_HEAPTUPLEHEADER(1);
	input = GetAttributeByName(v2, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_start_pos= DatumGetInt32(input); 
	input = GetAttributeByName(v2, "label", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v2_label = DatumGetInt32(input); 
	input = GetAttributeByName(v2, "cur", &isnull);
	if (isnull)
		v2_cur = NULL;
	else {
		isupdate = true;
		v2_cur = DatumGetArrayTypeP(input);
		ndims = ARR_DIMS(v2_cur)[0];
	}

	 if (PG_ARGISNULL(0) || isupdate==true) {
         	if (v2_cur == NULL) PG_RETURN_NULL();

		values[0] = Int32GetDatum(v2_start_pos);

		// generate label satisfy SimipleQuery1
		// if v2_start_pos > min(start_pos) where label=2
		// 	r_label != 1
		// if v2_start_pos < max(start_pos) where label=1 
		// 	r_label != 2
		// TODO: number of labels is hardcoded to 10
		
elog(DEBUG1, "***minidx_array(v2_cur,2) is %d***", minidx_array(v2_cur,2));
elog(DEBUG1, "***maxidx_array(v2_cur,1) is %d***", maxidx_array(v2_cur,1));
		
		if (minidx_array(v2_cur,2)!=-1 && v2_start_pos > minidx_array(v2_cur,2)) {
			r_label = 1;
			while (r_label == 1 || r_label > 10) 
				r_label = ((int)(drand48() * 10)) + 1;	
		} else if (maxidx_array(v2_cur,1)!=-1 && v2_start_pos < maxidx_array(v2_cur,1)) {
			r_label = 2;
			while (r_label == 2 || r_label > 10) 
				r_label = ((int)(drand48() * 10)) + 1;	
		} else {
			while (r_label > 10)
				r_label = ((int)(drand48() * 10)) + 1;
		}
		
		values[1] = Int32GetDatum(r_label);

		// TODO: optimize the above logic
		// we can cache the min and max to avoid 
		// computing every time, which require updating
		// if v2_cur[v2_start_pos] == 2
		// 	recompute min(start_pos) where label=2
		// if v2_cur[v2_start_pos] == 1
		// 	recompute max(start_pos) where label=1

		// update the current state matrix
                r_cur = DatumGetArrayTypePCopy(v2_cur);
		((int*)ARR_DATA_PTR(r_cur))[v2_start_pos] = r_label;
		values[2] = PointerGetDatum(r_cur);

		 if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE){
               ereport(ERROR,
                     (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("function returning record called in context that cannot accept type record")));
                        tupdesc = BlessTupleDesc(tupdesc);
       		 }

                 tup = heap_form_tuple(tupdesc, values, nulls);

                 result = HeapTupleGetDatum(tup);

                 PG_RETURN_DATUM(result);
	}


	// get running aggregate state
        HeapTupleHeader v1 = PG_GETARG_HEAPTUPLEHEADER(0);
	input = GetAttributeByName(v1, "start_pos", &isnull);
	if (isnull) 
		PG_RETURN_NULL();
	v1_start_pos= DatumGetInt32(input); 
        input = GetAttributeByName(v1, "label", &isnull);
        if (isnull)
                PG_RETURN_NULL();
        v1_label = DatumGetInt32(input);
        input = GetAttributeByName(v1, "cur", &isnull);
        if (isnull)
                v1_cur = NULL;
        else {
                v1_cur = DatumGetArrayTypeP(input);
                ndims = ARR_DIMS(v1_cur)[0];
        }

	// fill in the new values
	inputref = GetAttributeByRef(v1, "start_pos", &isnull);
	*((int32*)DatumGetPointer(inputref)) = v2_start_pos;

	// generate label satisfy SimipleQuery1
	// if v2_start_pos > min(start_pos) where label=2
	// 	r_label != 1
	// if v2_start_pos < max(start_pos) where label=1 
	// 	r_label != 2
	// TODO: number of labels is hardcoded to 10
	
elog(DEBUG1, "***minidx_array(v2_cur,2) is %d***", minidx_array(v1_cur,2));
elog(DEBUG1, "***maxidx_array(v2_cur,1) is %d***", maxidx_array(v1_cur,1));
		
	if (minidx_array(v1_cur,2)!=-1 && v2_start_pos > minidx_array(v1_cur,2)) {
		r_label = 1;
		while (r_label == 1 || r_label > 10) 
			r_label = ((int)(drand48() * 10)) + 1;	
	} else if (maxidx_array(v1_cur,1)!=-1 && v2_start_pos < maxidx_array(v1_cur,1)) {
		r_label = 2;
		while (r_label == 2 || r_label > 10) 
			r_label = ((int)(drand48() * 10)) + 1;	
	} else {
		while (r_label > 10)
			r_label = ((int)(drand48() * 10)) + 1;
	}
	
	inputref = GetAttributeByRef(v1, "label", &isnull);
	*((int32*)DatumGetPointer(inputref)) = r_label;

	// TODO: optimize the above logic
	// we can cache the min and max to avoid 
	// computing every time, which require updating
	// if v2_cur[v2_start_pos] == 2
	// 	recompute min(start_pos) where label=2
	// if v2_cur[v2_start_pos] == 1
	// 	recompute max(start_pos) where label=1
	
	// update current state matrix
	((int*)ARR_DATA_PTR(v1_cur))[v2_start_pos] = r_label;

        PG_RETURN_HEAPTUPLEHEADER(v1);

}
