/* ----------------------------------------------------------------------- *//** 
 *
 * @file plda.sql
 *
 * @brief SQL functions for parallel Latent Dirichlet Allocation
 * @sa For an introduction to Latent Dirichlet Allocation models, see the 
       module description \ref grp_lda.
 *
 *//* ------------------------------------------------------------------------*/

/**
@addtogroup grp_lda

@about

Latent Dirichlet Allocation (LDA) is an interesting generative probabilistic 
model for natural texts that has received a lot of attention in recent years. 
The model is quite versatile, having found uses in problems like automated 
topic discovery, collaborative filtering, and document classification.

The LDA model posits that each document has a mixture of various topics (e.g.
a document is related to Topic 1 with probability 0.7, and Topic 2 with 
probability 0.3), and that each word in the document is attributable to one 
of the document's topics. There is a (symmetric) Dirichlet prior with parameter 
\f$ \alpha \f$ on each document's topic mixture. In addition, there is another 
(symmateric) Dirichlet prior with parameter \f$ \eta \f$ on the distribution 
of words for each topic. The following generative process then defines a distribution 
over a corpus of documents. First sample a per-topic word distribution 
\f$ \Phi_i \f$, for each topic \f$ i \f$, from the \f$ Dirichlet(\eta) \f$ prior. 
Then for each document:
-# Sample a document length N from a suitable distribution, say, Poisson.
-# Sample a topic mixture \f$ \theta \f$ for the document from the \f$ Dirichlet(\alpha) \f$ distribution.
-# For each of the N words:
   -# Sample a topic \f$ z_n \f$ from the multinomial topic distribution \f$ \theta \f$.
   -# Sample a word \f$ w_n \f$ from the multinomial word distribution \f$ \Phi_{z_n} \f$ associated with topic \f$ z_n \f$.

In practice, only the words in each document are observable. The topic mixture of 
each document and the topic for each word in each document are latent unobservable 
variables that need to be inferred from the observables, and this is the problem
people refer to when they talk about the inference problem for LDA. Exact inference
is intractable, but several approximate inference algorithms for LDA have been
developed. The simple and effective Gibbs sampling algorithm described in 
Griffiths and Steyvers [2] appears to be the current algorithm of choice. Our 
parallel implementation of LDA comes from Wang et al [3], which is essentially
a straightforward parallelisation of the Gibbs sampling algorithm.

See also http://code.google.com/p/plda/.

@prereq

None 

@usage

Here is the main learning function.

-  Topic inference is achieved through the following Python function
   \code
   plda_run(numiter int, numtopics int, alpha float8, eta float8, restart bool)
   \endcode,
   where numiter is the number of iterations to run the Gibbs sampling, numtopics
   is the number of topics to discover, alpha is the parameter to the topic Dirichlet
   prior, eta is the parameter to the Dirichlet prior on the per-topic word distributions,
   and restart is a boolean value indicating whether we're restarting a previously
   terminated inference run. The plda_run() function needs to be run from within
   Python.

@examp

-# As a first step, we need to prepare and populate two tables/views with the following 
   structure:
   \code   
        madlib.lda_corpus (       
                id         INT,    -- document ID
                contents   INT[],  -- words in the document
		topics     madlib.topics_t  -- topic assignment to words
    	);
   \endcode
   and
   \code
	madlib.lda_dict (
		id         INT,    -- dictionary ID
		a          TEXT[]  -- array of words in the dictionary
        );
   \endcode
   The topics column of madlib.lda_corpus can be left empty to begin with. 
   The module comes with some randomly generated data. For example, we can import a 
   randomly generated list of documents using the command
   \code
        psql database_name -f importTestcases.sql
   \endcode
     
-# We can now kick-start the inference process by running the following inside a
   Python session:
   \code
        import plda
        plda.plda_run(100,8,0.5,0.5,False)
   \endcode
   If the program terminates without converging to a good solution, we can restart 
   the learning process where it terminated by running more iterations as follows: 
   \code
        plda_run(200,8,0.5,0.5,True)
   \endcode
   This restarting process can be run multiple times.

After a successful run of the plda_run() function, the results of learning can be
obtained by running the following inside the Greenplum Database.

-# The topic assignments for each document can be obtained as follows:
   \code
	select id, (topics).topics from madlib.lda_corpus;
   \endcode

-# The number of times words in each document were assigned to each topic can be obtained as follows:
   \code
	select id, (topics).topic_d from madlib.lda_corpus;
   \endcode

-# The number of times each word was assigned to a topic can be computed as follows:
   \code
	select ss.i, madlib.wordTopicDistrn(gcounts,$numtopics,ss.i) from madlib.globalWordTopicCount, (select generate_series(1,$dictsize) i) as ss where mytimestamp = $num_iter ;
   \endcode

-# The total number of words assigned to each topic can be computed as follows:
   \code
	select sum((topics).topic_d) topic_sums from madlib.lda_corpus;
   \endcode

@literature

[1] D.M. Blei, A.Y. Ng, M.I. Jordan, <em>Latent Dirichlet Allocation</em>, 
    Journal of Machine Learning Research, vol. 3, pp. 993-1022, 2003.

[2] T. Griffiths and M. Steyvers, <em>Finding scientific topics</em>, 
    PNAS, vol. 101, pp. 5228-5235, 2004.

[3] Y. Wang, H. Bai, M. Stanton, W-Y. Chen, and E.Y. Chang, <em>PLDA: 
    Parallel Dirichlet Allocation for Large-scale Applications</em>, AAIM, 2009.

[4] http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation
*/


-- \timing
\i plda_drop.sql

-- drop schema madlib cascade ;
-- create schema madlib;

-- The topics_t data type store the assignment of topics to each word in a document,
-- plus the distribution of those topics in the document.
CREATE TYPE madlib.topics_t AS (
       topics int4[],
       topic_d int4[]
);

CREATE TABLE madlib.lda_corpus ( id int4, contents int4[], topics madlib.topics_t ) DISTRIBUTED BY (id);

CREATE TABLE madlib.lda_dict ( id int4, a text[] ) DISTRIBUTED RANDOMLY;

CREATE OR REPLACE FUNCTION madlib.dictsize(OUT ret INT4) AS $$
DECLARE 
	lastidx int4;
BEGIN
	SELECT INTO lastidx array_upper(a,1) FROM madlib.lda_dict WHERE id = 1000000;
	SELECT INTO ret a[lastidx] FROM madlib.lda_dict WHERE id = 1000000;
END;
$$ LANGUAGE plpgsql;

-- Returns the word at a given index 
CREATE OR REPLACE FUNCTION madlib.dict_word(idx int4, OUT ret text) AS $$
BEGIN
	SELECT INTO ret a[idx] FROM madlib.lda_dict WHERE id = 1000000;
END;
$$ LANGUAGE plpgsql;

-- \i importSmall.sql
-- \i importTestcases.sql
-- \i importBible.sql

-- Returns a C-style array 
CREATE OR REPLACE FUNCTION madlib.setup2dArray(int4,int4) RETURNS bytea
AS 'plda_support.so', 'setup2dArray' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION madlib.getEle(bytea,int4,int4,int4) RETURNS int4
AS 'plda_support.so', 'getEle' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION madlib.setEle(bytea,int4,int4,int4,int4) RETURNS void
AS 'plda_support.so', 'setEle' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION madlib.arr_size(bytea) RETURNS int4
AS 'plda_support.so', 'arr_size' LANGUAGE C STRICT;

/*
CREATE OR REPLACE FUNCTION madlib.default_array(d int4, ele int4, OUT ret int4[]) AS $$
BEGIN
	FOR i IN 1..d LOOP
	    ret[i] = ele;
	END LOOP;
END;
$$ LANGUAGE plpgsql;
*/

CREATE OR REPLACE FUNCTION 
madlib.zero_array(d int4) RETURNS int4[] 
AS 'plda_support.so', 'zero_array' LANGUAGE C STRICT;

-- Returns an array of random topic assignments for a given document length
CREATE OR REPLACE FUNCTION 
madlib.randomTopics(doclen int4, numtopics int4, OUT ret madlib.topics_t) AS $$
DECLARE
	rtopic INT4;			 
BEGIN
	FOR i IN 1..numtopics LOOP ret.topic_d[i] := 0; END LOOP;

	FOR i IN 1..doclen LOOP
	    rtopic := trunc(random() * numtopics + 1);
	    ret.topics[i] := rtopic;
	    ret.topic_d[rtopic] := ret.topic_d[rtopic] + 1;
	END LOOP;
END;
$$ LANGUAGE plpgsql;


-- This function assigns a randomly chosen topic to each word in a document according to 
-- the count statistics obtained for the document and the whole corpus so far. 
CREATE OR REPLACE FUNCTION
madlib.randomAssignTopic_sub(int4,int4[],int4[],int4[],bytea,int4[],int4,int4[],int4[],float,float) RETURNS void
AS 'plda_support.so', 'randomAssignTopic_sub' LANGUAGE C STRICT;

-- This function assigns a topic to each word in a document according to the count
-- statistics obtained on the whole corpus so far.
-- Parameters
--   doc     : the document to be analysed
--   doc_topics   : the topics of each word in the doc and their distributions
--   global_count : the global word-topic counts; the type is a C-style 2d array
--   topic_counts : the counts of all words in the corpus in each topic
--   num_topics   : number of topics to be discovered
--
CREATE OR REPLACE FUNCTION 
madlib.randomAssignTopic(doc int4[], doc_topics madlib.topics_t, global_count bytea, topic_counts int4[], num_topics int4, alpha float, eta float)
RETURNS madlib.topics_t AS $$
DECLARE
	len int4;
	ret madlib.topics_t;
	widx int4;
	rtopic int4;
BEGIN
	len := array_upper(doc,1);
	ret.topics := madlib.zero_array(len); 
	ret.topic_d := madlib.zero_array(num_topics); 

	PERFORM madlib.randomAssignTopic_sub(len,doc,doc_topics.topics,doc_topics.topic_d,
			                   global_count,topic_counts,num_topics,
					   ret.topics,ret.topic_d,alpha,eta);
	RETURN ret;
END;
$$ LANGUAGE plpgsql; 

-- This is just a place-holder for a bytea; seems to be necessary as the state in aggregate functions;
-- can get into all kinds of strange error if we don't have this wrapper.
CREATE TYPE madlib.global_counts AS (
       mycounts bytea -- int[][]
);

CREATE OR REPLACE FUNCTION madlib.init_global_counts(dict_size int4, nclasses int4) RETURNS madlib.global_counts AS $$
DECLARE
	ret madlib.global_counts;
BEGIN
	ret.mycounts := madlib.setup2dArray(dict_size, nclasses);
	RETURN ret;
END;
$$ LANGUAGE plpgsql;

-- Updates mycounts (the 2d word-topic counts) with the topic assignments for given doc 
CREATE OR REPLACE FUNCTION madlib.cword_count2(len int4, doc int4[], topics int4[], mycounts bytea, num_topics int4) RETURNS VOID
AS 'plda_support.so', 'cword_count2' LANGUAGE C STRICT; 

-- Computes the per document word-topic counts
CREATE OR REPLACE FUNCTION madlib.cword_count(mystate madlib.global_counts, doc int4[], topics int4[], num_topics int4, dsize int4) 
RETURNS madlib.global_counts AS $$
DECLARE
	len int4;
BEGIN
	IF mystate IS NULL THEN 
	   mystate := madlib.init_global_counts(dsize,num_topics); 
	END IF;

	len := array_upper(doc,1);
	-- mystate.mycounts := madlib.cword_count2(len, doc, topics, mystate.mycounts, num_topics);
	PERFORM madlib.cword_count2(len, doc, topics, mystate.mycounts, num_topics);

	RETURN mystate;
END;
$$ LANGUAGE plpgsql;

-- Aggregate function to compute all word-topic counts given topic assignments for each document
CREATE AGGREGATE madlib.cword_agg(int4[], int4[], int4, int4) (
       sfunc = madlib.cword_count,
       stype = madlib.global_counts
);

-- This stores the local word-topic counts computed at each segment 
CREATE TABLE madlib.localWordTopicCount ( id int4, mytimestamp int4, mycounts madlib.global_counts )
DISTRIBUTED BY (mytimestamp);

-- This stores the global word-topic counts computed by summing the local word-topic counts computed
CREATE TABLE madlib.globalWordTopicCount ( mytimestamp int4, gcounts bytea ) -- gcounts int4[][]
DISTRIBUTED BY (mytimestamp); -- RANDOMLY;  

-- Computes the sum of two word-topic counts
CREATE OR REPLACE FUNCTION madlib.sumArray(bytea, bytea, int4) RETURNS void
AS 'plda_support', 'sumArray' LANGUAGE C STRICT;

-- State transition function to compute the sum of local word-topic counts
CREATE OR REPLACE FUNCTION madlib.sum2darray(mystate madlib.global_counts, local_count madlib.global_counts, num_topics int4, dictsize int4) RETURNS madlib.global_counts AS $$
DECLARE
	newval int4;
BEGIN
	IF mystate IS NULL THEN
	   mystate := madlib.init_global_counts(dictsize, num_topics);
	END IF;

	PERFORM madlib.sumArray(mystate.mycounts, local_count.mycounts, num_topics * dictsize);
	RETURN mystate;
END;
$$ LANGUAGE plpgsql;

-- Aggregate function to compute the sum of local word-topic counts
CREATE AGGREGATE madlib.sum2darrays(madlib.global_counts, int4, int4) (
       sfunc = madlib.sum2darray,
       stype = madlib.global_counts
);

-- The main parallel LDA learning function
CREATE OR REPLACE FUNCTION madlib.plda(num_topics int4, num_iter int4, init_iter int4, alpha float, eta float) 
RETURNS int4 AS $$
DECLARE
	dsize int4;           -- size of the dictionary
	topic_counts int4[];  -- total number of words in each topic
	glwcounts bytea;     -- global word-topic count
	temp int4;
	iter int4;
BEGIN
	SELECT INTO topic_counts SUM((topics).topic_d) FROM madlib.lda_corpus;
	IF topic_counts IS NULL THEN
	    RAISE NOTICE 'Error: topic_counts not initialised properly';
	END IF;

	-- Get the dictionary
	dsize := madlib.dictsize();
	IF dsize = 0 OR dsize IS NULL THEN
	    RAISE NOTICE 'Error: dictionary has not been initialised.';
	END IF;
	RAISE NOTICE 'dsize = %', dsize;

	-- Get global word-topic counts computed from the previous call if available
	SELECT INTO temp count(*) FROM madlib.globalWordTopicCount WHERE mytimestamp = init_iter;
	IF temp = 1 THEN
	    RAISE NOTICE 'Found global word-topic count from a previous call';
	    SELECT INTO glwcounts gcounts FROM madlib.globalWordTopicCount WHERE mytimestamp = init_iter;
        ELSE
	    RAISE NOTICE 'Initialising global word-topic count for the very first time';
	    glwcounts := madlib.setup2dArray(dsize, num_topics);
	END IF;

	-- Clear the local and global word-topic counts from the previous call
	DELETE FROM madlib.localWordTopicCount;
	DELETE FROM madlib.globalWordTopicCount;

	FOR i in 1..num_iter LOOP
	    iter := i + init_iter;
	    RAISE NOTICE 'Starting iteration %', iter;
	    -- Randomly reassign topics to the words in each document, in parallel; the map step
	    UPDATE madlib.lda_corpus SET topics = madlib.randomAssignTopic(contents,topics,glwcounts,topic_counts,num_topics,alpha,eta);

	    -- Compute the local word-topic counts in parallel; the map step
	    INSERT INTO madlib.localWordTopicCount 
	       (SELECT gp_segment_id,iter,madlib.cword_agg(contents,(topics).topics,num_topics,dsize)
    	   	FROM madlib.lda_corpus GROUP BY gp_segment_id);

	    -- Compute the global word-topic counts; the reduce step
	    INSERT INTO madlib.globalWordTopicCount 
	       (SELECT iter, (madlib.sum2darrays(mycounts, num_topics, dsize)).mycounts 
	    	FROM madlib.localWordTopicCount WHERE mytimestamp = iter);

	    SELECT INTO glwcounts gcounts FROM madlib.globalWordTopicCount WHERE mytimestamp = iter;
	    -- <<check glwcounts assignment>>

	    -- Compute the denominator
	    SELECT INTO topic_counts SUM((topics).topic_d) FROM madlib.lda_corpus;
			     
	    RAISE NOTICE '  Done iteration %', iter;
   	END LOOP;

	RETURN init_iter + num_iter;
END;
$$ LANGUAGE plpgsql;

-- After running plda(), here are the ways to compute the relevant return values from the 
-- corresponding R function 
--
-- D = number of documents
-- V = the size of the vocabulary
-- K = the number of topics
--
-- assignments - A list of length D, where each element of the list is an integer vector 
--               giving the topic assignments to words in the corresponding document.
--  query: select id, (topics).topics from madlib.lda_corpus ;
--
-- topics - A K x V matrix where each entry indicates the number of times a word (column)
--          was assigned to a topic (row).
--  query: select ss.i, madlib.wordTopicDistrn(gcounts,$numtopics,ss.i) from madlib.globalWordTopicCount, (select generate_series(1,$dictsize) i) as ss where mytimestamp = $num_iter ;
--
-- topic_sums : A length K vector where each entry indicates the total number of times words
--              were assigned to each topic.
--  query: select sum((topics).topic_d) topic_sums from madlib.lda_corpus;
--
-- document_sums : A K x D matrix where each entry is an integer indicating the number of 
--                 times words in each document were assigned to each topic.
--  query: select id, (topics).topic_d from madlib.lda_corpus;
-- 


/* <<check glwcounts assignment>>
   	    FOR j IN 1..num_topics LOOP
	    	SELECT INTO word_col madlib.getColumn(j, gcounts, dsize, num_topics) FROM madlib.globalWordTopicCount WHERE mytimestamp = i;
	    	FOR k in 1..dsize LOOP
		    -- glwcounts[k][j] := word_col[k];
		    -- PERFORM madlib.setEle(glwcounts,k,j,num_topics,word_col[k]);

		    IF madlib.getEle(glwcounts,k,j,num_topics) <> word_col[k] THEN
		       RAISE NOTICE 'Error2: glwcounts[%][%] % <> word_col[k] %', k, j, madlib.getEle(glwcounts,k,j,num_topics), word_col[k];
		    END IF;

		END LOOP;
	    END LOOP;
*/
/*
CREATE OR REPLACE FUNCTION madlib.getColumn(idx int4, twodarray bytea, dsize int4, ntopics int4, OUT ret int4[]) AS $$ -- twodarray int4[][]
BEGIN
	FOR i in 1..dsize LOOP
	    ret[i] := madlib.getEle(twodarray,i,idx,ntopics); 	-- ret[i] := twodarray[i][idx];
	END LOOP;
END;
$$ LANGUAGE plpgsql;
*/

-- Returns the array of topic distribution of a given word 
CREATE OR REPLACE FUNCTION madlib.wordTopicDistrn(arr bytea, ntopics int4, word int4) 
RETURNS int4[] 
AS $$
DECLARE 
	ret int4[];
BEGIN
	FOR i in 1..ntopics LOOP
	    ret[i] := madlib.getEle(arr,word,i,ntopics);
	END LOOP;
	RETURN ret;
END;
$$ LANGUAGE plpgsql;

CREATE TYPE madlib.word_distrn AS ( word text, distrn int4[] );
 
-- Returns the word-topicDistribution pairs for each word in the dictionary
CREATE OR REPLACE FUNCTION 
madlib.wordTopicDistributions( ltimestamp int4, num_topics int4)
RETURNS SETOF madlib.word_distrn AS $$
DECLARE
	distrn INT4[];
	total INT4 := 0;
	glbcounts bytea;
	dict text[];
	dsize int4;
	maxprob float4;
	tempval float4;
	ret madlib.word_distrn;
BEGIN
	SELECT INTO glbcounts gcounts FROM madlib.globalWordTopicCount WHERE mytimestamp = ltimestamp; 
	SELECT INTO dict a FROM madlib.lda_dict WHERE id = 1000000;
	dsize := array_upper(dict,1);

	FOR i IN 1..dsize LOOP
	    total := 0;
	    SELECT INTO distrn madlib.wordTopicDistrn(glbcounts,num_topics,i);
	    FOR j IN 1..num_topics LOOP
	    	total := total + distrn[j];
	    END LOOP;

	    ret.word := dict[i];
	    ret.distrn := distrn;
	    RETURN NEXT ret;

	END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE TYPE madlib.word_weight AS ( word text, prob float, wcount int4 );

-- Returns the most important words for each topic
CREATE OR REPLACE FUNCTION 
madlib.getImportantWords( ltimestamp int4, topicid int4, num_topics int4)
RETURNS SETOF madlib.word_weight AS $$
DECLARE
	distrn INT4[];
	total INT4 := 0;
	glbcounts bytea;
	dict text[];
	dsize INT4;
	wdprob float4;
	ret madlib.word_weight;
	mincount int4;
BEGIN
	SELECT INTO glbcounts gcounts FROM madlib.globalWordTopicCount WHERE mytimestamp = ltimestamp; 
	SELECT INTO dict a FROM madlib.lda_dict WHERE id = 1000000;
	dsize := array_upper(dict,1);

	FOR i IN 1..dsize LOOP
	    total := 0;
	    SELECT INTO distrn madlib.wordTopicDistrn(glbcounts,num_topics,i);
	    FOR j IN 1..num_topics LOOP
	    	total := total + distrn[j];
	    END LOOP;
	    IF total = 0 THEN 
	        -- RAISE NOTICE 'Word % has zero count', i;
	        CONTINUE; 
	    END IF;

	    mincount := 2;
	    -- mincount := 10;
	    IF num_topics > mincount THEN mincount := num_topics; END IF;
	    wdprob := distrn[topicid] * 1.0 / total;

	    IF total > mincount AND wdprob > 0 THEN
	        ret.word := dict[i];
		ret.prob := wdprob; 
		ret.wcount := total;
		RETURN NEXT ret;
	    END IF;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION 
madlib.predictiveDistribution(docid int4, num_topics int4, last_iter int4, alpha float8, eta float8) 
RETURNS SETOF madlib.word_weight AS $$
DECLARE
	dsize int4;
	dict text[];
	topic_distrn int4[];
	distrn INT4[];
	glwcounts bytea;
	topic_sum INT4[];
	word_prob float8;
	ret madlib.word_weight;
BEGIN
	SELECT INTO dict a FROM madlib.lda_dict WHERE id = 1000000;
	dsize := array_upper(dict,1);
	SELECT INTO glwcounts gcounts FROM madlib.globalWordTopicCount WHERE mytimestamp = last_iter;
	SELECT INTO topic_sum sum((topics).topic_d) topic_sums FROM madlib.lda_corpus;
	SELECT INTO topic_distrn (topics).topic_d FROM madlib.lda_corpus WHERE id = docid;
	
	FOR j in 1..dsize LOOP
	    word_prob := 0;
	    FOR i in 1..num_topics LOOP
		 word_prob := word_prob + (topic_distrn[i] + alpha) * 
		 	      ((madlib.getEle(glwcounts,j,i,num_topics) * 1.0)/topic_sum[i] + eta); 
	    END LOOP;
	    ret.word := dict[j];
	    ret.prob := word_prob;
	    RETURN NEXT ret;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

/*
[Their LDA usage]
As for the case of document analysis, the flow is like following.

Japanese doc --> (Morphological Analysis)--> (some normalization)
--> LDA(In LDA, dimension compression will be done.)

They have made parallelised "Morphological Analysis" and "normalization"
already. So the only thing that they are not able to parallelise is LDA.

And they told us that if your PLDA would be like following that would be
nice.

Input:
- TABLE : (dimension number, word number) x (document number)
   The value is 'count'.
   <notice: Input data is often sparse, so generally it would be
    "Incidence matrix" form>
- the seed of random number
   "Sensitive Dependence on Initial Conditions" is there.
   So, hopefully, if we can specify seed, that would be great.
- the number of class (topic )
- the number of iteration
- data set for training ( same form as the first one)

Output:
- model data
    (word(dimension) number) x (class(topic) number)
    The value is 'strength'.
    In incidence matrix form,
     model data(word int4,class int4[],value double precision[])

- inference data
    (document number) x (class(topic) number)
    The value is 'strength'.
    In incidence matrix form,
     inference data(docID int4, class int4[], value double precision[])

- the log likelihood variation x trial
   (the number of trial) x (log likelihood)


[Their answer for your questions]----
* What's the size of the dictionary (i.e. unique number of words
      in the corpus) the engineers at NTT are dealing with? Is it in
      the thousands, tens of thousands?

Their ans:
This would be the number of dimension.
In natural language, we think about 3million would be max.
However, in our usage, there is the case from the hundreds to the thousands.

* How many distinct documents are they analysing? What's the
      average number of words in each document?

Their ans:
We assume the number of document would be very large.
That would be from the millions to hundreds of millions.
As to training set, from tens of thousands to hundreds of thousands
would be ok.

As to the average number of words in each document,
it is case by case, however, those would be sparse.
Because, basically, 3million word would not be filled.
So you can think it is sparse.

* Are the documents in Japanese or English?

Their ans: Japanese

* What's the current limitations of the R implementation? What's the
      maximum number of documents they can analyse? How long does
      that take?

Their ans:
R is not parallelised.
As to max num of doc,
the answer for this is the same as second question.
The document which will be analyzed by LDA is 100% unique because
(some normalization) does 'distinct'.
------------------------------------------------------
*/


/*
20,000 docs, 2 iterations, 3 topics: 8.5 s
*/