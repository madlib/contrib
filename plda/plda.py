"""
@file plda.py

@brief Parallel LDA: Driver function
@author Kee Siong Ng

Parallel LDA: Driver function
"""

import plpy

# ----------------------------------------
# Quotes a string to be used as a literal 
# ----------------------------------------
def quote_literal(val):
    return "'" + val.replace("'", "''") + "'";

# Each document is represented as an array of integers, with each integer
# representing a word. Word integers must start from 1.

def plda_run(datatable, dicttable, modeltable, outputdatatable, numiter, numtopics, alpha, eta, restart):
    """
    Executes the parallel LDA algorithm.

    @param datatable       Name of table/view containing the input data points
    @param dicttable       Name of table/view containing the alphabet dictionary
    @param modeltable      Name of table to store the word-topic counts
    @param outputdatatable Name of table to store the topic assignments to each document in datatable
    @param numiter         Number of iterations to run the Gibbs sampling
    @param numtopics       Number of topics to discover
    @param alpha           Parameter of the Dirichlet distribution for document topic mixtures
    @param eta             Parameter of the Dirichlet distribution for per-topic word distributions
    @param restart         This is True if we want to continue from a previously terminated run
    """

    plpy.connect('testdb', 'localhost', 5432, 'gpadmin', 'password')
    # plpy.execute('set client_min_messages=info')

    # This stores the local word-topic counts computed at each segment 
    sql = '''CREATE TEMP TABLE localWordTopicCount ( id int4, mytimestamp int4, lcounts int4[] ) ''' + '''
             DISTRIBUTED BY (mytimestamp)'''
    plpy.execute(sql)

    # This stores the global word-topic counts
    sql = '''CREATE TABLE ''' + modeltable + ''' ( mytimestamp int4, gcounts int4[] ) ''' + '''
             DISTRIBUTED BY (mytimestamp)'''
    plpy.execute(sql)

    # This store a copy of the corpus of documents to be analysed
    sql = '''CREATE TABLE ''' + outputdatatable + ''' 
             ( id int4, contents int4[], topics madlib.lda_topics_t ) DISTRIBUTED RANDOMLY'''
    plpy.execute(sql)

    restartstep = 0
    if (restart == False):
        plpy.execute("SELECT setseed(0.5)")
        
        plpy.info('Copying training data into tables ' + outputdatatable)
        sql = '''INSERT INTO ''' + outputdatatable + ''' 
                 (SELECT id, contents FROM ''' + datatable + ''')'''
        plpy.execute(sql)
        plpy.info('  .... Done')

        plpy.info('Assigning initial random topics to documents in the corpus')
        sql = '''UPDATE ''' + outputdatatable + ''' 
                 SET topics = madlib.lda_random_topics(array_upper(contents,1),''' + str(numtopics) + ''')'''
        plpy.execute(sql)
        plpy.info('  .... Done')
    else:
        rv = plpy.execute("SELECT MAX(mytimestamp) FROM " + modeltable)
        restartstep = rv[0]['max']
        numiter = numiter - restartstep    

    # Get number of words in dictionary    
    dsize_t = plpy.execute("SELECT array_upper(dict,1) dsize FROM " + dicttable)
    dsize = dsize_t[0]['dsize']

    # The number of iterations to do per call to lda_train(); can increase to around 10
    stepperround = 2
    numrounds = numiter / stepperround
    leftover = numiter % stepperround

    plpy.info('Starting learning process')
    for i in range(0,numrounds):

        plpy.execute("SELECT madlib.lda_train(" + str(dsize) + "," + str(numtopics) + "," 
                               + str(stepperround) +"," + str(restartstep + i*stepperround) + "," 
                               + str(alpha) + "," + str(eta) + ", 'localWordTopicCount', '" 
                               + modeltable + "', '" + outputdatatable + "')")

        plpy.info( '  ... finished iteration %d' % (restartstep + (i+1)*stepperround))
        plpy.execute("VACUUM " + outputdatatable)

    if leftover > 0:

        plpy.execute("SELECT madlib.lda_train(" + str(dsize) + "," + str(numtopics) + "," 
                               + str(leftover) + "," + str(restartstep + numrounds*stepperround) + "," 
                               + str(alpha) + "," + str(eta) + ", 'localWordTopicCount', '" 
                               + modeltable + "', '" + outputdatatable + "')")

    # Clean up    
    last_iter = restartstep + numrounds*stepperround + leftover
    plpy.execute("DELETE FROM " + modeltable + " WHERE mytimestamp < " + str(last_iter))
    plpy.execute('DROP TABLE localWordTopicCount')

    # Print the most probable words in each topic
    for i in range(1,numtopics+1):
        rv = plpy.execute("select * from madlib.lda_topic_word_prob(" 
                                            + str(numtopics) + "," + str(i) + "," + str(last_iter) 
                                            + ", '" + modeltable + "', '" + outputdatatable + "', '" 
                                            + dicttable + "') order by -prob limit 20")
        plpy.info( 'Topic %d' % i)
        for j in range(0,min(len(rv),20)):
            word = rv[j]['word']
            prob = rv[j]['prob']
            count = rv[j]['wcount']
            plpy.info( ' %d) %s   \t %f \t %d' % (j+1, word, prob, count))

# Example usage
def plda_test():
    plpy.connect('testdb', 'localhost', 5432, 'gpadmin', 'password')
    plpy.execute('drop table if exists madlib.lda_mymodel')
    plpy.execute('drop table if exists madlib.lda_corpus')
    plpy.execute('drop table if exists madlib.lda_testcorpus')
    plpy.execute('create table madlib.lda_testcorpus ( id int4, contents int4[], topics madlib.lda_topics_t ) distributed randomly')
    plpy.execute('insert into madlib.lda_testcorpus (select * from madlib.lda_mycorpus limit 20)')
    plda_run('madlib.lda_mycorpus', 'madlib.lda_mydict', 'madlib.lda_mymodel', 'madlib.lda_corpus', 30,10,0.5,0.5,False)
    plpy.execute("select madlib.lda_label_test_documents('madlib.lda_testcorpus', 'madlib.lda_mymodel', 'madlib.lda_corpus', 'madlib.lda_mydict', 30,10,0.5,0.5)")
    plpy.execute("select * from madlib.lda_testcorpus")


plda_test()


