/* ----------------------------------------------------------------------- *//** 
 *
 * @file plda_support.c
 *
 * @brief Support functions for Parallel Latent Dirichlet Allocation
 * @author Kee Siong Ng
 *
 * This file is a subsidiary file to plda.sql, where we implement a few important
 * UDFs in PL/C for efficiency reasons.
 *
 * Word-topic counts, which are 2 dimensional matrices, are implemented as a 
 * one dimensional array using Postgres's internal ArrayType data structure.
 *
 *//* ----------------------------------------------------------------------- */

#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include <stdlib.h>
#include <assert.h>

PG_MODULE_MAGIC;

/* Indicate "version 1" calling conventions for all exported functions. */
PG_FUNCTION_INFO_V1(randomAssignTopic_sub);
PG_FUNCTION_INFO_V1(zero_array);
PG_FUNCTION_INFO_V1(cword_count);

/**
 * Returns an array of a given length filled with zeros
 */
Datum zero_array(PG_FUNCTION_ARGS);
Datum zero_array(PG_FUNCTION_ARGS)
{
	int32 len = PG_GETARG_INT32(0);
	Datum * array = palloc0(len * sizeof(Datum));
	ArrayType * pgarray = construct_array(array, len, INT4OID, 4, true, 'i');
	PG_RETURN_ARRAYTYPE_P(pgarray);
}

/**
 * This function updates the word-topic count array given the assignment of
 * topics to words in a document.
 *
 * Note: The function modifies the input word-topic count array, and can only 
 * be used as part of the cword_agg() function.
 */
Datum cword_count(PG_FUNCTION_ARGS);
Datum cword_count(PG_FUNCTION_ARGS)
{
	ArrayType * count_arr, * doc_arr, * topics_arr;
	int32 * count, * doc, * topics;
	int32 doclen, num_topics, dsize, i;
	Datum * array;
	int32 idx;

	doclen = PG_GETARG_INT32(3);
	num_topics = PG_GETARG_INT32(4);
	dsize = PG_GETARG_INT32(5);

	/* Construct a zero'd array at the first call of this function */
	if (PG_ARGISNULL(0)) {
		array = palloc0(dsize*num_topics*sizeof(Datum));
		count_arr =
		    construct_array(array,dsize*num_topics,INT4OID,4,true,'i');
	} else {
		count_arr = PG_GETARG_ARRAYTYPE_P(0);
	}
	doc_arr = PG_GETARG_ARRAYTYPE_P(1);
	topics_arr = PG_GETARG_ARRAYTYPE_P(2);

	/* Check that the input arrays are of the right dimension and type */
	if (ARR_NDIM(count_arr) != 1 || ARR_ELEMTYPE(count_arr) != INT4OID ||
	    ARR_NDIM(doc_arr) != 1 || ARR_ELEMTYPE(doc_arr) != INT4OID ||
	    ARR_NDIM(topics_arr) != 1 || ARR_ELEMTYPE(topics_arr) != INT4OID)
		ereport
		 (ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("transition function \"%s\" called with invalid parameters",
			  format_procedure(fcinfo->flinfo->fn_oid))));

	count = (int32 *)ARR_DATA_PTR(count_arr);
	doc = (int32 *)ARR_DATA_PTR(doc_arr);
	topics = (int32 *)ARR_DATA_PTR(topics_arr);

	/* Update the word-topic count */
	for (i=0; i!=doclen; i++) {
		idx = (doc[i]-1) * num_topics + (topics[i]-1);

		if (idx < 0 || idx >= dsize*num_topics)
			ereport
			 (ERROR,
			  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		           errmsg("function \"%s\" called with invalid parameters",
				  format_procedure(fcinfo->flinfo->fn_oid))));
		
		count[idx]++;
	}
	PG_RETURN_BYTEA_P(count_arr);
}

/**
 * This function samples a new topic for a given word based on count statistics
 * computed on the rest of the corpus. This is the core function in the Gibbs
 * sampling inference algorithm for LDA. 
 * 
 * Parameters
 *  @param numtopics number of topics
 *  @param widx the index of the current word whose topic is to be sampled
 *  @param wtopic the current assigned topic of the word
 *  @param global_count the word-topic count matrix
 *  @param local_d the distribution of topics in the current document
 *  @param topic_counts the distribution of number of words in the corpus assigned to each topic
 *  @param alpha the Dirichlet parameter for the topic multinomial
 *  @param eta the Dirichlet parameter for the per-topic word multinomial
 *
 * The function is non-destructive to all the input arguments.
 */
static int32 sampleTopic
   (int32 numtopics, int32 widx, int32 wtopic, int32 * global_count,
    int32 * local_d, int32 * topic_counts, float8 alpha, float8 eta) 
{
	int32 j, glcount_temp, locald_temp, ret;
	float8 r, cl_prob, total_unpr;

	// this array captures the cumulative prob. distribution of the topics
	float8 * topic_prs = (float8 *)palloc(sizeof(float8) * numtopics); 

	/* make adjustment for 0-indexing */
	widx--;
	wtopic--;

	/* calculate topic (unnormalised) probabilities */
	total_unpr = 0;
	for (j=0; j!=numtopics; j++) {
		// number of times widx is assigned topic j in the corpus
		glcount_temp = global_count[widx * numtopics + j];
		// number of times a word is assigned topic j in this document
		locald_temp = local_d[j];
		// adjust the counts to exclude current word's contribution
		if (j == wtopic) {
			glcount_temp--;
			locald_temp--;
		}
		// the topic probability for current word, proportional to 
		//   fraction of times word is assigned topic j
		// x fraction of times a word is assigned topic j in current doc
		cl_prob = (locald_temp + alpha) * (glcount_temp + eta) /
			  (topic_counts[j] + numtopics * eta);
		total_unpr += cl_prob;
		topic_prs[j] = total_unpr;
	}
	/* normalise probabilities */
	for (j=0; j!=numtopics; j++)
		topic_prs[j] = topic_prs[j] / total_unpr;

	/* Draw a topic at random */
	r = (random() * 1.0) / RAND_MAX;
	ret = 1;
	while (true) {
		if (ret == numtopics || r < topic_prs[ret-1]) break;
		ret++; 
	}
	if (ret < 1 || ret > numtopics)
		elog(ERROR, "sampleTopic: ret = %d", ret);

	pfree(topic_prs);
	return ret;
}

/**
 * This function assigns a topic to each word in a document using the count
 * statistics obtained so far on the corpus. The function returns an array
 * of int4s, which pack two arrays together: the topic assignment to each
 * word in the document (the first len elements in the returned array), and
 * the number of words assigned to each topic (the last num_topics elements
 * of the returned array).
 */
Datum randomAssignTopic_sub(PG_FUNCTION_ARGS);
Datum randomAssignTopic_sub(PG_FUNCTION_ARGS)
{
	int32 i, widx, wtopic, rtopic;
	int32 * doc, * topics, * topic_d, * global_count, * topic_counts;
	int32 num_topics, dsize;
	float8 alpha, eta;
	Datum * array;
	ArrayType * ret_arr;
	int32 * ret;

	// length of document
	int32 len = PG_GETARG_INT32(0);

	ArrayType * doc_arr = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType * topics_arr = PG_GETARG_ARRAYTYPE_P(2);
	ArrayType * topic_d_arr = PG_GETARG_ARRAYTYPE_P(3);
	ArrayType * global_count_arr = PG_GETARG_ARRAYTYPE_P(4);
	ArrayType * topic_counts_arr = PG_GETARG_ARRAYTYPE_P(5);

	if (ARR_NULLBITMAP(doc_arr) || ARR_NDIM(doc_arr) != 1 || 
	    ARR_ELEMTYPE(doc_arr) != INT4OID ||
	    ARR_NDIM(topics_arr) != 1 || ARR_ELEMTYPE(topics_arr) != INT4OID ||
	    ARR_NULLBITMAP(topic_d_arr) || ARR_NDIM(topic_d_arr) != 1 || 
	    ARR_ELEMTYPE(topic_d_arr) != INT4OID ||
	    ARR_NULLBITMAP(global_count_arr) || ARR_NDIM(global_count_arr) != 1
	    || ARR_ELEMTYPE(global_count_arr) != INT4OID ||
	    ARR_NULLBITMAP(topic_counts_arr) || ARR_NDIM(topic_counts_arr) != 1
	    || ARR_ELEMTYPE(topic_counts_arr) != INT4OID)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("function \"%s\" called with invalid parameters",
				format_procedure(fcinfo->flinfo->fn_oid))));

	// the document array
	doc = (int32 *)ARR_DATA_PTR(doc_arr);

	// array giving topic assignment to each word in document
	topics = (int32 *)ARR_DATA_PTR(topics_arr);

	// distribution of topics in document
	topic_d = (int32 *)ARR_DATA_PTR(topic_d_arr);

	// the word-topic count matrix
	global_count = (int32 *)ARR_DATA_PTR(global_count_arr);

	// total number of words assigned to each topic in the whole corpus
	topic_counts = (int32 *)ARR_DATA_PTR(topic_counts_arr);

	num_topics = PG_GETARG_INT32(6);
	dsize = PG_GETARG_INT32(7);

	// Dirichlet parameters
	alpha = PG_GETARG_FLOAT8(8);
	eta = PG_GETARG_FLOAT8(9);

	array = palloc0((len+num_topics) * sizeof(Datum));
	ret_arr = construct_array(array, len+num_topics, INT4OID, 4,
					      true, 'i');
	ret = (int32 *)ARR_DATA_PTR(ret_arr);

	for (i=0; i!=len; i++) {
		widx = doc[i];

		if (widx < 1 || widx > dsize)
			ereport
			 (ERROR,
			  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			   errmsg("function \"%s\" called with invalid parameters",
				  format_procedure(fcinfo->flinfo->fn_oid))));

		wtopic = topics[i];
		rtopic = sampleTopic(num_topics,widx,wtopic,global_count,
				     topic_d,topic_counts,alpha,eta);

		// <randomAssignTopic_sub error checking> 

		ret[i] = rtopic;
		ret[len-1+rtopic]++;
	}
	PG_RETURN_ARRAYTYPE_P(ret_arr);
}
/*
 <randomAssignTopic_sub error checking>
 if (rtopic < 1 || rtopic > num_topics || wtopic < 1 || wtopic > num_topics)
     elog(ERROR, 
	  "randomAssignTopic_sub: rtopic = %d wtopic = %d", rtopic, wtopic);
 */


