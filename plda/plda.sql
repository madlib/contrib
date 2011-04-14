/* ----------------------------------------------------------------------- *//** 
 *
 * @file plda.sql
 *
 * @brief SQL functions for parallel Latent Dirichlet Allocation
 * @author Kee Siong Ng
 *
 * @sa For an introduction to Latent Dirichlet Allocation models, see the 
       module description \ref grp_lda.
 *
 *//* ------------------------------------------------------------------------*/

/**

@addtogroup grp_lda

@about
\section about About

Latent Dirichlet Allocation (LDA) is an interesting generative probabilistic 
model for natural texts that has received a lot of attention in recent years. 
The model is quite versatile, having found uses in problems like automated 
topic discovery, collaborative filtering, and document classification.

The LDA model posits that each document is associated with a mixture of various 
topics (e.g. a document is related to Topic 1 with probability 0.7, and Topic 2 with 
probability 0.3), and that each word in the document is attributable to one 
of the document's topics. There is a (symmetric) Dirichlet prior with parameter 
\f$ \alpha \f$ on each document's topic mixture. In addition, there is another 
(symmateric) Dirichlet prior with parameter \f$ \eta \f$ on the distribution 
of words for each topic. The following generative process then defines a distribution 
over a corpus of documents. First sample, for each topic \f$ i \f$, a per-topic word distribution 
\f$ \Phi_i \f$ from the Dirichlet(\f$\eta\f$) prior. 
Then for each document:
-# Sample a document length N from a suitable distribution, say, Poisson.
-# Sample a topic mixture \f$ \theta \f$ for the document from the Dirichlet(\f$\alpha\f$) distribution.
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

@installation
\section installation Installation

   To install the module, run the following command in the plda source directory to compile 
   the shared object file.
   \code
   make install
   \endcode
   If the machine is a multi-node machine, we need to copy plda_support.so to all the segment
   hosts as follows (you may need to do this as gpadmin):
   \code
   gpscp -f /path/to/host_file plda_support.so =:$GPHOME/lib/postgresql/
   \endcode
   We can then install the parallel LDA user-defined functions as follows:
   \code
   psql db_name -f plda.sql
   \endcode
   To uninstall the parallel LDA module, we do this:
   \code
   psql db_name -f plda_drop.sql
   \endcode

@usage
\section usage Application Interface

This is the main learning function.

-  Topic inference is achieved through the following Python function
   \code
   plda_run(datatable, dicttable, numiter int, numtopics int, 
            alpha float8, eta float8, restart bool),
   \endcode
   where datatable is the table where the corpus to be analysed is stored (the table is
   assumed to have the two columns: id int and contents int[]), dicttable is the table
   where the dictionary is stored (the table is assumed to have the column dict text[],
   and contains only one row), numiter is the number of iterations to run the Gibbs sampling, 
   numtopics is the number of topics to discover, alpha is the parameter to the topic Dirichlet
   prior, eta is the parameter to the Dirichlet prior on the per-topic word distributions,
   and restart is a boolean value indicating whether we're restarting a previously
   terminated inference run. The plda_run() function needs to be run from within
   Python.

There is another function that will allow users to predict the labels of new documents using
the LDA model learned from a training corpus. This has been written but is still subject to 
further testing. This function, implemented as madlib.labelTestDocuments() in plda.sql, should 
be used with caution for now.

@examp
\section example Example

We now give a usage example.

-# As a first step, we need to prepare and populate two tables/views with the following 
   structure:
   \code   
   corpus_table_name (       
         id         INT4,    -- document ID
         contents   INT4[],  -- words in the document
	 ....                -- other fields
   );
   dictionary_table_name ( a TEXT[] ); -- words in the dictionary 
   \endcode
   The module comes with some randomly generated data. For example, we can import two
   test tables madlib.mycorpus and madlib.mydict using the command
   \code
   psql database_name -f importTestcases.sql
   \endcode     
-# We can now kick-start the inference process by running the following.
   \code
   python
   >>> import plda
   >>> plda.plda_run('madlib.mycorpus', 'madlib.mydict', 20,10,0.5,0.5,False)
   \endcode
   If the program terminates without converging to a good solution, we can restart 
   the learning process where it terminated by running more iterations as follows: 
   \code
   >>> plda.plda_run('madlib.mycorpus', 'madlib.mydict', 50,10,0.5,0.5,True)
   \endcode
   This restarting process can be run multiple times. 


After a successful run of the plda_run() function, the most probable words associated 
with each topic are displayed. Other results of the learning process can be obtained 
by running the following commands inside the Greenplum Database.

-# The topic assignments for each document can be obtained as follows:
   \code
   select id, (topics).topics from madlib.lda_corpus;
   \endcode
-# The topic distribution of each document can be obtained as follows:
   \code
   select id, (topics).topic_d from madlib.lda_corpus;
   \endcode
-# The number of times each word was assigned to a given topic in the whole corpus can 
   be computed as follows:
   \code
   select ss.i, madlib.wordTopicDistrn(gcounts,$numtopics,ss.i) 
   from madlib.globalWordTopicCount, 
        (select generate_series(1,$dictsize) i) as ss 
   where mytimestamp = $num_iter ;
   \endcode
   where $numtopics is the number of topics used in the learning process, and 
   $num_iter is the number of iterations ran in the learning process.
-# The total number of words assigned to each topic in the whole corpus can be computed 
   as follows:
   \code
   select sum((topics).topic_d) topic_sums from madlib.lda_corpus;
   \endcode

\section notes Additional Notes

The input format for the Parallel LDA module is different from that used by the 
`lda' package for R. In the `lda' package, each document is represented by two
equal dimensional integer arrays. The first array represents the words the occur
in the document, and the second array captures the number of times each word in
the first array occurs in the document.
This representation appears to have a major weakness in that all the occurrences
of a word in a document must be assigned the same topic, which is clearly not
satisfactory. Further, at the time of writing, the main learning function in the 
`lda' package actually does not work correctly when the occurrence counts for words 
aren't one.

A major limitation of the module is that, at present, only one session of plda_run() 
can be executed in each database. This is because each plda_run() stores its learning 
result inside the madlib.lda_corpus table, and multiple plda_run()'s will attempt to 
write to the same table and thus corrupt the intermediate learning result. This will
be fixed in a future version.

There is a script called generateTestCases.cc that can be used to generate some
simple test documents to validate the correctness and effiency of the parallel
LDA implementation.
 
@literature
\section literature Literature

Here are some relevant references.

[1] D.M. Blei, A.Y. Ng, M.I. Jordan, <em>Latent Dirichlet Allocation</em>, 
    Journal of Machine Learning Research, vol. 3, pp. 993-1022, 2003.

[2] T. Griffiths and M. Steyvers, <em>Finding scientific topics</em>, 
    PNAS, vol. 101, pp. 5228-5235, 2004.

[3] Y. Wang, H. Bai, M. Stanton, W-Y. Chen, and E.Y. Chang, <em>PLDA: 
    Parallel Dirichlet Allocation for Large-scale Applications</em>, AAIM, 2009.

[4] http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation

[5] J. Chang, Collapsed Gibbs sampling methods for topic models, R manual, 2010.

*/

-- \i plda_drop.sql

-- The topics_t data type store the assignment of topics to each word in a document,
-- plus the distribution of those topics in the document.
CREATE TYPE madlib.topics_t AS (
       topics int4[],
       topic_d int4[]
);

-- The table where the corpus of documents to be analysed sits
CREATE TABLE madlib.lda_corpus ( id int4, contents int4[], topics madlib.topics_t ) DISTRIBUTED RANDOMLY; -- BY (id);

-- The table where the corpus of test documents to be labelled sits
CREATE TABLE madlib.lda_testcorpus ( id int4, contents int4[], topics madlib.topics_t ) DISTRIBUTED RANDOMLY; -- BY (id);

-- The table for storing dictionary.
CREATE TABLE madlib.lda_dict ( id int4, a text[] ) DISTRIBUTED RANDOMLY;

-- Returns the size of the dictionary
CREATE OR REPLACE FUNCTION madlib.dictsize(OUT ret INT4) AS $$
	SELECT array_upper(a,1) FROM madlib.lda_dict WHERE id = 1000000;
$$ LANGUAGE sql;

-- Returns the word at a given index 
CREATE OR REPLACE FUNCTION madlib.dict_word(idx int4, OUT ret text) AS $$
	SELECT a[$1] FROM madlib.lda_dict WHERE id = 1000000;
$$ LANGUAGE sql;

-- Returns a zero'd array of a given dimension
CREATE OR REPLACE FUNCTION madlib.zero_array(d int4) RETURNS int4[] 
AS 'plda_support.so', 'zero_array' LANGUAGE C STRICT;

-- Returns an array of random topic assignments for a given document length
CREATE OR REPLACE FUNCTION madlib.randomTopics(doclen int4, numtopics int4) RETURNS madlib.topics_t 
AS 'plda_support.so', 'randomTopics' LANGUAGE C STRICT;

-- This function assigns a randomly chosen topic to each word in a document according to 
-- the count statistics obtained for the document and the whole corpus so far. 
-- Parameters
--   doc     : the document to be analysed
--   topics  : the topics of each word in the doc
--   topic_d : the topic distribution
--   global_count : the global word-topic counts
--   topic_counts : the counts of all words in the corpus in each topic
--   num_topics   : number of topics to be discovered
--   dsize        : size of dictionary
--   alpha        : the parameter of the Dirichlet distribution
--   eta          : the parameter of the Dirichlet distribution
--
CREATE OR REPLACE FUNCTION
madlib.sampleNewTopics(doc int4[], topics int4[], topic_d int4[], global_count int4[],
		       topic_counts int4[], num_topics int4, dsize int4, alpha float, eta float) 
RETURNS madlib.topics_t
AS 'plda_support.so', 'sampleNewTopics' LANGUAGE C STRICT;

-- Computes the per document word-topic counts
CREATE OR REPLACE FUNCTION 
madlib.cword_count(mystate int4[], doc int4[], topics int4[], doclen int4, num_topics int4, dsize int4)
RETURNS int4[]
AS 'plda_support.so', 'cword_count' LANGUAGE C;

-- Aggregate function to compute all word-topic counts given topic assignments for each document
CREATE AGGREGATE madlib.cword_agg(int4[], int4[], int4, int4, int4) (
       sfunc = madlib.cword_count,
       stype = int4[] 
);

-- This stores the local word-topic counts computed at each segment 
CREATE TABLE madlib.localWordTopicCount ( id int4, mytimestamp int4, lcounts int4[] )
DISTRIBUTED BY (mytimestamp);

-- This stores the global word-topic counts computed by summing the local word-topic counts computed
CREATE TABLE madlib.globalWordTopicCount ( mytimestamp int4, gcounts int4[] ) 
DISTRIBUTED BY (mytimestamp); -- RANDOMLY;  

-- The main parallel LDA learning function
CREATE OR REPLACE FUNCTION
madlib.plda(num_topics int4, num_iter int4, init_iter int4, alpha float, eta float) 
RETURNS int4 AS $$
	topic_counts_t = plpy.execute("select sum((topics).topic_d) tc from madlib.lda_corpus");
	if (topic_counts_t.nrows() == 0):
	    plpy.error("error: topic_counts not initialised properly");
	topic_counts = topic_counts_t[0]['tc'];

	# Get dictionary size
	dsize_t = plpy.execute("select array_upper(a,1) ds from madlib.lda_dict where id = 1000000");
	if (dsize_t.nrows() == 0):
	    plpy.error("error: dictionary has not been initialised");
	dsize = dsize_t[0]['ds'];
	plpy.info('dsize = %d' % dsize);

	# Get global word-topic counts computed from the previous call if available
	temp_t = plpy.execute("select count(*) temp from madlib.globalWordTopicCount where mytimestamp = " + str(init_iter));
	temp = temp_t[0]['temp'];

	if (temp == 1):
	    plpy.info('Found global word-topic count from a previous call');
	    glwcounts_t = plpy.execute("select gcounts[1:" + str(dsize*num_topics) + "] glwcounts from madlib.globalWordTopicCount where mytimestamp = " + str(init_iter));
	    glwcounts = glwcounts_t[0]['glwcounts'];
	else:
	    plpy.info('Initialising global word-topic count for the very first time');
	    glwcounts_t = plpy.execute("select madlib.zero_array(" + str(dsize*num_topics) + ") glwcounts");
	    glwcounts = glwcounts_t[0]['glwcounts'];

	# Clear the local and global word-topic counts from the previous call    
	plpy.execute('delete from madlib.localWordTopicCount');
	plpy.execute('delete from madlib.globalWordTopicCount');

	for i in range(1,num_iter+1):
	    iter = i + init_iter;
	    plpy.info('Starting iteration %d' % iter);
	    # Randomly reassign topics to the words in each document, in parallel; the map step
	    plpy.execute("update madlib.lda_corpus set topics = madlib.sampleNewTopics(contents,(topics).topics,(topics).topic_d,'" + str(glwcounts) + "','" + str(topic_counts) + "'," + str(num_topics) + "," + str(dsize) + "," + str(alpha) + "," + str(eta) + ")");
	    
	    # Compute the local word-topic counts in parallel; the map step
	    plpy.execute("insert into madlib.localWordTopicCount (select gp_segment_id, " + str(iter) + ", madlib.cword_agg(contents,(topics).topics,array_upper(contents,1)," + str(num_topics) + "," + str(dsize) + ") from madlib.lda_corpus group by gp_segment_id)");  

	    # Compute the global word-topic counts; the reduce step
	    plpy.execute("insert into madlib.globalWordTopicCount (select " + str(iter) + ", sum(lcounts) from madlib.localWordTopicCount where mytimestamp = " + str(iter) + ")");
	    
	    glwcounts_t = plpy.execute("select gcounts[1:" + str(dsize*num_topics) + "] glwcounts from madlib.globalWordTopicCount where mytimestamp = " + str(iter));
	    glwcounts = glwcounts_t[0]['glwcounts'];

	    # Compute the denominator
	    topic_counts_t = plpy.execute("select sum((topics).topic_d) tc from madlib.lda_corpus");
	    topic_counts = topic_counts_t[0]['tc'];

	    plpy.info('  Done iteration %d' % iter);

	return init_iter + num_iter;   
$$ LANGUAGE plpythonu;

-- Returns the distribution of topics for a given word
CREATE OR REPLACE FUNCTION madlib.wordTopicDistrn(arr int4[], ntopics int4, word int4, OUT ret int4[]) AS $$
       SELECT $1[(($3-1)*$2 + 1):(($3-1)*$2 + $2)];
$$ LANGUAGE sql;

CREATE TYPE madlib.word_distrn AS ( word text, distrn int4[], prob float8[] );
 
-- Returns the word-topicDistribution pairs for each word in the dictionary
CREATE OR REPLACE FUNCTION 
madlib.wordTopicDistributions( ltimestamp int4, num_topics int4)
RETURNS SETOF madlib.word_distrn AS $$
DECLARE
	distrn INT4[];
	prob FLOAT8[];
	total INT4 := 0;
	glbcounts int4[]; 
	dict text[];
	dsize int4;
	maxprob float4;
	tempval float4;
	ret madlib.word_distrn;
BEGIN
	SELECT INTO dict a FROM madlib.lda_dict WHERE id = 1000000;
	IF NOT FOUND THEN
	   RAISE NOTICE 'Error: failed to find dictionary';
	END IF;
	dsize := array_upper(dict,1);

	SELECT INTO glbcounts gcounts[1:dsize*num_topics] FROM madlib.globalWordTopicCount WHERE mytimestamp = ltimestamp; 
	IF NOT FOUND THEN
	   RAISE NOTICE 'Error: failed to find gcounts for time step %', ltimestamp;
	END IF;
	
	FOR i IN 1..dsize LOOP
	    total := 0;
	    SELECT INTO distrn madlib.wordTopicDistrn(glbcounts,num_topics,i);
	    IF NOT FOUND THEN
	       RAISE NOTICE 'Error: failed to find topic distribution for word %', i;
	    END IF;
	    FOR j IN 1..num_topics LOOP
	    	total := total + distrn[j];
	    END LOOP;

	    IF total = 0 THEN CONTINUE; END IF;

	    FOR j IN 1..num_topics LOOP
	    	prob[j] := distrn[j] * 1.0 / total;
	    END LOOP;

	    ret.word := dict[i];
	    ret.distrn := distrn;
	    ret.prob := prob;
	    RETURN NEXT ret;

	END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE TYPE madlib.word_weight AS ( word text, prob float, wcount int4 );

-- Returns the most important words for each topic, base on Pr( word | topic ).
CREATE OR REPLACE FUNCTION madlib.topicWordProb(num_topics int4, topic int4, ltimestamp int4) RETURNS SETOF madlib.word_weight AS $$
DECLARE
	glbcounts int4[];
	topic_sum int4[];
	dict text[];
	dsize int4;
	ret madlib.word_weight;
BEGIN
	SELECT INTO dict a FROM madlib.lda_dict WHERE id = 1000000;
	IF NOT FOUND THEN
	   RAISE NOTICE 'Error: failed to find dictionary';
	END IF;
	dsize := array_upper(dict,1);

	SELECT INTO glbcounts gcounts[1:dsize*num_topics] FROM madlib.globalWordTopicCount WHERE mytimestamp = ltimestamp; 
	IF NOT FOUND THEN
	   RAISE NOTICE 'Error: failed to find gcounts for time step %', ltimestamp;
	END IF;
	SELECT INTO topic_sum sum((topics).topic_d) topic_sums FROM madlib.lda_corpus;
	IF NOT FOUND THEN
	   RAISE NOTICE 'Error: failed to compute topic_sum';
	END IF;
	

	FOR i IN 1..dsize LOOP
	    ret.word := dict[i];
	    ret.wcount := glbcounts[(i-1)*num_topics + topic];
	    ret.prob := ret.wcount * 1.0 / topic_sum[topic];
	    RETURN NEXT ret;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Returns the most important words for each topic, based on Pr( topic | word ).
CREATE OR REPLACE FUNCTION 
madlib.getImportantWords( ltimestamp int4, topicid int4, num_topics int4)
RETURNS SETOF madlib.word_weight AS $$
DECLARE
	distrn INT4[];
	total INT4 := 0;
	glbcounts INT4[]; -- bytea;
	dict text[];
	dsize INT4;
	wdprob float4;
	ret madlib.word_weight;
	mincount int4;
BEGIN
	SELECT INTO dict a FROM madlib.lda_dict WHERE id = 1000000;
	IF NOT FOUND THEN
	   RAISE NOTICE 'getImportantWords: dictionary not found';
	END IF;
	dsize := array_upper(dict,1);

	SELECT INTO glbcounts gcounts[1:dsize*num_topics] FROM madlib.globalWordTopicCount WHERE mytimestamp = ltimestamp; 
	IF glbcounts IS NULL THEN
	   RAISE NOTICE 'getImportantWords: glbcounts = NULL';
	END IF;

	mincount = 10;	    
	IF num_topics > mincount THEN mincount := num_topics; END IF;

	FOR i IN 1..dsize LOOP
	    total := 0;
	    SELECT INTO distrn madlib.wordTopicDistrn(glbcounts,num_topics,i);
	    IF NOT FOUND THEN
	       RAISE NOTICE 'getImportantWords: topic distribution for word % not found', i;
	    END IF;
	    FOR j IN 1..num_topics LOOP
	    	total := total + distrn[j];
	    END LOOP;

	    IF total <= mincount THEN CONTINUE; END IF;

	    wdprob := distrn[topicid] * 1.0 / total;

	    IF total > mincount AND wdprob > 1.0/num_topics THEN
	        ret.word := dict[i];
		ret.prob := wdprob; 
		ret.wcount := total;
		RETURN NEXT ret;
	    END IF;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

-- This function computes the topic assignments to words in a document given previously computed
-- statistics from the training corpus. This function and the next are still being tested.
CREATE OR REPLACE FUNCTION 
madlib.labelDocument(doc int4[], global_count int4[], topic_counts int4[], num_topics int4, dsize int4, 
		     alpha float, eta float)
RETURNS madlib.topics_t AS $$
DECLARE
	ret madlib.topics_t;
BEGIN
	ret := madlib.randomTopics(array_upper(doc,1), num_topics);
	FOR i in 1..20 LOOP
	    ret := madlib.sampleNewTopics(doc,(ret).topics,(ret).topic_d,global_count,topic_counts,num_topics,dsize,alpha,eta);
    	END LOOP;
	RETURN ret;
END;
$$ LANGUAGE plpgsql;

-- This function computes the topic assignments to documents in a test corpus named madlib.lda_testcorpus
CREATE OR REPLACE FUNCTION
madlib.labelTestDocuments(ltimestep int4, num_topics int4, alpha float, eta float)
RETURNS VOID AS $$
DECLARE
	glwcounts int4[];
	topic_counts int4[];
	dsize int4;
BEGIN
	dsize := madlib.dictsize();
	SELECT INTO glwcounts gcounts[1:dsize*num_topics] FROM madlib.globalWordTopicCount WHERE mytimestamp = ltimestep;
	IF NOT FOUND THEN
	   RAISE NOTICE 'labelTestDocuments: gcounts for time step % not found', ltimestep;
	END IF;
	SELECT INTO topic_counts SUM((topics).topic_d) FROM madlib.lda_corpus;
	IF NOT FOUND THEN
	   RAISE NOTICE 'labelTestDocuments: failed to compute topic_counts';
	END IF;

	UPDATE madlib.lda_testcorpus 
	SET topics = madlib.labelDocument(contents, glwcounts, topic_counts, num_topics, dsize,
	    	     		          alpha, eta);
END;
$$ LANGUAGE plpgsql;



