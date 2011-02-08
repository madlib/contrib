/* ----------------------------------------------------------------------- *//** 
 *
 * @file plda_support.c
 *
 * @brief Support functions for Parallel Latent Dirichlet Allocation
 * @author Kee Siong Ng
 *
 *//* ----------------------------------------------------------------------- */

#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include <stdlib.h>
#include <assert.h>

PG_MODULE_MAGIC;

/* Indicate "version 1" calling conventions for all exported functions. */
PG_FUNCTION_INFO_V1(arr_size);
PG_FUNCTION_INFO_V1(setup2dArray);
PG_FUNCTION_INFO_V1(getEle);
PG_FUNCTION_INFO_V1(setEle);
PG_FUNCTION_INFO_V1(cword_count2);
PG_FUNCTION_INFO_V1(sumArray);
PG_FUNCTION_INFO_V1(randomAssignTopic_sub);
PG_FUNCTION_INFO_V1(default_array);

/**
 * Returns an array of a given length filled with a certain given value.
 */
Datum default_array(PG_FUNCTION_ARGS);
Datum default_array(PG_FUNCTION_ARGS)
{
	// int16 elemtyplen;
	// bool elemtypbyval;
	// char elemtypalign;
	Datum * array;
	int i;

	ArrayType * pgarray;
	int32 len = PG_GETARG_INT32(0);
	int32 val = PG_GETARG_INT32(1);
	/*
	get_typlenbyvalalign(INT4OID,
			     &elemtyplen,
			     &elemtypbyval,
			     &elemtypalign);
	*/
	array = palloc(len * sizeof(Datum));
	for (i=0; i!=len; i++) array[i] = val;

	// elog(NOTICE, "%lu %lu", sizeof(Datum), sizeof(int32));

	// pgarray = construct_array(array, len, INT4OID, elemtyplen,
	//			  elemtypbyval, elemtypalign);	
	pgarray = construct_array(array, len, INT4OID, 4, true, 'i');

	pfree(array);
	PG_RETURN_ARRAYTYPE_P(pgarray);
}

/**
 * Returns the size of a C-style 1d array
 */
Datum arr_size(PG_FUNCTION_ARGS);
Datum arr_size(PG_FUNCTION_ARGS)
{
	bytea * in = PG_GETARG_BYTEA_P(0);
	int32 ret = (VARSIZE(in) - VARHDRSZ) / sizeof(int32);
	PG_RETURN_INT32(ret);
}

/** 
 * Returns a C-style 1d array to represent a 2d array
 */
Datum setup2dArray(PG_FUNCTION_ARGS);
Datum setup2dArray(PG_FUNCTION_ARGS)
{
	int32 nwords = PG_GETARG_INT32(0);
	int32 ntopics = PG_GETARG_INT32(1);
	int32 bsize = VARHDRSZ + nwords * ntopics * sizeof(int32);
	bytea * out = (bytea *)palloc0(bsize);

	if (out == NULL)
		elog(ERROR, "setup2dArray: failed to allocate enough memory");

	SET_VARSIZE(out, bsize);
	PG_RETURN_BYTEA_P(out);
}

/**
 * Returns a particular element in the input 2d array; indexing starts from 1.
 * The first index is the word index; the second, the topic index.
 */
Datum getEle(PG_FUNCTION_ARGS);
Datum getEle(PG_FUNCTION_ARGS)
{
	bytea * arr = PG_GETARG_BYTEA_P(0);
	int32 i = PG_GETARG_INT32(1) - 1;
	int32 j = PG_GETARG_INT32(2) - 1;
	int32 ntopics = PG_GETARG_INT32(3);
	int32 nwords = VARSIZE(arr) / (ntopics * sizeof(int32));
	int32 * vals = (int32 *)VARDATA(arr);

	if (i >= nwords || i < 0 || j >= ntopics || j < 0) 
		elog(ERROR, "getEle: index out of bounds %d %d", i, j);

	PG_RETURN_INT32(vals[i*ntopics + j]);
}

/**
 * Sets the value of a cell in the input 2d array; indexing starts from 1.
 * The first index is the word index; the second, the topic index.
 *
 * Note: This function is destructive to the input 2d array and should be
 *       used with that in mind.
 */
Datum setEle(PG_FUNCTION_ARGS);
Datum setEle(PG_FUNCTION_ARGS)
{
	bytea * arr = PG_GETARG_BYTEA_P(0);
	int32 i = PG_GETARG_INT32(1) - 1;
	int32 j = PG_GETARG_INT32(2) - 1;
	int32 ntopics = PG_GETARG_INT32(3);
	int32 newval = PG_GETARG_INT32(4);

	int32 nwords = VARSIZE(arr) / (ntopics * sizeof(int32));
	int32 * vals = (int32 *)VARDATA(arr);

	if (i >= nwords || i < 0 || j >= ntopics || j < 0) 
		elog(ERROR, "index out of bounds");

	vals[i*ntopics + j] = newval;

	PG_RETURN_VOID();
}

/**
 * This function updates the input word-topic 2d matrix given a document
 * and the topic assignments for each word in the document.
 *
 * NOTE: The function is destructive to the input word-topic matrix and
 *       should be used with this in mind.
 */
Datum cword_count2(PG_FUNCTION_ARGS);
Datum cword_count2(PG_FUNCTION_ARGS)
{
	int32 len = PG_GETARG_INT32(0);
	ArrayType * arr1 = PG_GETARG_ARRAYTYPE_P(1);
	int32 * doc = (int32 *)ARR_DATA_PTR(arr1);

	ArrayType * arr2 = PG_GETARG_ARRAYTYPE_P(2);
	int32 * topics = (int32 *)ARR_DATA_PTR(arr2);

	bytea * arr3 = PG_GETARG_BYTEA_P(3);
	int32 * wordTopicCounts = (int32 *)VARDATA(arr3);

	int32 num_topics = PG_GETARG_INT32(4);
	int32 i;
	// int32 j;

	/*
	for (i=0; i!=len; i++)
		if (doc[i] > 10001 || doc[i] < 0)
			elog(ERROR, "doc[%d] = %d", i, doc[i]);

	for (i=0; i!=len; i++)
		if (topics[i] < 1 || topics[i] > num_topics)
			elog(ERROR, "topics[%d] = %d", i, topics[i]);
	
	j = (VARSIZE(arr3) - VARHDRSZ) / sizeof(int32);
	if (j != 786 * num_topics)
		elog(ERROR, "%d != %d", j, 786 * num_topics);

	for (i=0; i!=786 * num_topics; i++)
		if (wordTopicCounts[i] < 0 || wordTopicCounts[i] > 1000000)
			elog(ERROR, "wordTopicCounts[%d] = %d", i, wordTopicCounts[i]);
	*/
	for (i=0; i!=len; i++) {
		/*
		j = (doc[i]-1) * num_topics + (topics[i]-1);
		if (j < 0 || j > 786 * num_topics)
			elog(ERROR, "j = %d", j);
		*/
		wordTopicCounts[(doc[i]-1) * num_topics + (topics[i]-1)]++;
	}
	// PG_RETURN_BYTEA_P(arr3);
	PG_RETURN_VOID();
}

/**
 * This function adds two arrays together and stores the result in the 
 * first array.
 *
 * NOTE: This function is destructive to the first array and should be
 *       used with this in mind.
 */
Datum sumArray(PG_FUNCTION_ARGS);
Datum sumArray(PG_FUNCTION_ARGS)
{
	bytea * arr1 = PG_GETARG_BYTEA_P(0);
	bytea * arr2 = PG_GETARG_BYTEA_P(1);

	int32 * gcount = (int32 *)VARDATA(arr1);
	int32 * lcount = (int32 *)VARDATA(arr2);

	int32 len = PG_GETARG_INT32(2);
	int32 i;

	for (i=0; i!=len; i++)
		gcount[i] = gcount[i] + lcount[i];

	PG_RETURN_VOID();
}

/**
 * This function samples a new topic for a given word based on count statistics
 * computed on the rest of the corpus. This is the core function in the Gibbs
 * sampling inference algorithm for LDA. 
 * 
 * Parameters
 *  numtopics    : number of topics
 *  widx         : the index of the current word whose topic is to be sampled
 *  wtopic       : the current assigned topic of the word
 *  global_count : the word-topic count matrix
 *  local_d      : the distribution of topics in the current document
 *  topic_counts : the distribution of number of words in the corpus assigned 
 *                 to each topic
 *  alpha        : the Dirichlet parameter for the topic multinomial
 *  eta          : the Dirichlet parameter for the per-topic word multinomial
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
		if (r < topic_prs[ret-1]) break;
		ret++; 
	}
	if (ret < 1 || ret > numtopics)
		elog(ERROR, "sampleTopic: ret = %d", ret);

	pfree(topic_prs);
	return ret;
}

/**
 * This function assigns a topic to each word in a document using the count
 * statistics obtained so far on the corpus.
 *
 */
Datum randomAssignTopic_sub(PG_FUNCTION_ARGS);
Datum randomAssignTopic_sub(PG_FUNCTION_ARGS)
{
	int32 i, widx, wtopic, rtopic;

	// length of document
	int32 len = PG_GETARG_INT32(0);

	// the document array
	ArrayType * doc_arr = PG_GETARG_ARRAYTYPE_P(1);
	int32 * doc = (int32 *)ARR_DATA_PTR(doc_arr);

	// array giving topic assignment to each word in document
	ArrayType * topics_arr = PG_GETARG_ARRAYTYPE_P(2);
	int32 * topics = (int32 *)ARR_DATA_PTR(topics_arr);

	// distribution of topics in document
	ArrayType * topic_d_arr = PG_GETARG_ARRAYTYPE_P(3);
	int32 * topic_d = (int32 *)ARR_DATA_PTR(topic_d_arr);

	// the word-topic count matrix
	bytea * global_count_bytea = PG_GETARG_BYTEA_P(4);
	int32 * global_count = (int32 *)VARDATA(global_count_bytea);

	// total number of words assigned to each topic in the whole corpus
	ArrayType * topic_counts_arr = PG_GETARG_ARRAYTYPE_P(5);
	int32 * topic_counts = (int32 *)ARR_DATA_PTR(topic_counts_arr);

	int32 num_topics = PG_GETARG_INT32(6);

	// the zero array for the new topic assignments to be returned
	ArrayType * ret_topics_arr = PG_GETARG_ARRAYTYPE_P(7);
	int32 * ret_topics = (int32 *)ARR_DATA_PTR(ret_topics_arr);

	// the zero array for the new document topic distribution to be returned
	ArrayType * ret_topic_d_arr = PG_GETARG_ARRAYTYPE_P(8);
	int32 * ret_topic_d = (int32 *)ARR_DATA_PTR(ret_topic_d_arr);

	// Dirichlet parameters
	float8 alpha = PG_GETARG_FLOAT8(9);
	float8 eta = PG_GETARG_FLOAT8(10);

	for (i=0; i!=len; i++) {
		widx = doc[i];
		wtopic = topics[i];
		rtopic = sampleTopic(num_topics,widx,wtopic,global_count,
				     topic_d,topic_counts,alpha,eta);

		/* <randomAssignTopic_sub error checking> */

		ret_topics[i] = rtopic;
		ret_topic_d[rtopic-1]++; // adjust for 0-indexing
	}
	PG_RETURN_VOID();
}

/*
 <randomAssignTopic_sub error checking>
 if (rtopic < 1 || rtopic > num_topics || wtopic < 1 || wtopic > num_topics)
     elog(ERROR, 
	  "randomAssignTopic_sub: rtopic = %d wtopic = %d", rtopic, wtopic);
 */
