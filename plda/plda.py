"""
@file plda.py

@brief Parallel LDA: Driver function
@author Kee Siong Ng

Parallel LDA: Driver function
"""

import plpy

# Each document is represented as an array of integers, with each integer
# representing a word. Word integers must start from 1.

def plda_run(datatable, dicttable, modeltable, outputdatatable, numiter, numtopics, alpha, eta, restart):
    """
    Executes the parallel LDA algorithm.

    @param datatable  Name of table/view containing the input data points
    @param dicttable  Name of table/view containing the alphabet dictionary
    @param modeltable Name of table to store the word-topic counts
    @param outputdatatable Name of table to store the topic assignments to each document in datatable
    @param numiter    Number of iterations to run the Gibbs sampling
    @param numtopics  Number of topics to discover
    @param alpha      Parameter of the Dirichlet distribution for document topic mixtures
    @param eta        Parameter of the Dirichlet distribution for per-topic word distributions
    @param restart    This is True if we want to continue from a previously terminated run
    """

    plpy.connect('testdb', 'localhost', 5432, 'gpadmin', 'password')
    # plpy.execute('set client_min_messages=info')

    # This stores the local word-topic counts computed at each segment 
    plpy.execute('CREATE TEMP TABLE localWordTopicCount ( id int4, mytimestamp int4, lcounts int4[] ) DISTRIBUTED BY (mytimestamp)')
    # This stores the global word-topic counts
    plpy.execute('CREATE TABLE ' + modeltable + ' ( mytimestamp int4, gcounts int4[] ) DISTRIBUTED BY (mytimestamp)')
    # This store a copy of the corpus of documents to be analysed
    plpy.execute('CREATE TABLE ' + outputdatatable + ' ( id int4, contents int4[], topics madlib.topics_t ) DISTRIBUTED RANDOMLY')

    restartstep = 0
    if (restart == False):
        plpy.execute("SELECT setseed(0.5)")
        
        plpy.info('Copying training data into tables ' + outputdatatable + ' and ' + dicttable)
        plpy.execute("INSERT INTO " + outputdatatable + " (SELECT id, contents FROM " + datatable + ")")
        plpy.info('  .... Done')

        plpy.info('Assigning initial random topics to documents in the corpus')
        plpy.execute("UPDATE " + outputdatatable + " SET topics = madlib.randomTopics(array_upper(contents,1)," + str(numtopics) + ")")
        plpy.info('  .... Done')
    else:
        rv = plpy.execute("SELECT MAX(mytimestamp) FROM " + modeltable)
        restartstep = rv[0]['max']
        numiter = numiter - restartstep    

    dsize_t = plpy.execute("SELECT array_upper(dict,1) dsize FROM " + dicttable)
    dsize = dsize_t[0]['dsize']

    stepperround = 2
    numrounds = numiter / stepperround
    leftover = numiter % stepperround

    plpy.info('Starting learning process')
    for i in range(0,numrounds):
        plpy.info( 'Starting iteration')
        plpy.execute("select madlib.plda(" + str(dsize) + "," + str(numtopics) + "," + str(stepperround) +"," + str(restartstep + i*stepperround) + "," + str(alpha) + "," + str(eta) + ", 'localWordTopicCount', '" + modeltable + "', '" + outputdatatable + "')")
        plpy.info( 'Finished iteration %d' % (restartstep + (i+1)*stepperround))
        plpy.execute("VACUUM " + outputdatatable)

    if leftover > 0:
        plpy.execute("select madlib.plda(" + str(dsize) + "," + str(numtopics) + "," + str(leftover) + "," + str(restartstep + numrounds*stepperround) + "," + str(alpha) + "," + str(eta) + ", 'localWordTopicCount', '" + modeltable + "', '" + outputdatatable + "')")

    plpy.info('Finished learning process')     

    plpy.execute('DROP TABLE localWordTopicCount')

    rv = plpy.execute("SELECT MAX(mytimestamp) FROM " + modeltable)
    finalstep = rv[0]['max']    
    for i in range(1,numtopics+1):
        rv = plpy.execute("select * from madlib.topicWordProb(" + str(numtopics) + "," + str(i) + "," + str(finalstep) + ", '" + modeltable + "', '" + outputdatatable + "', '" + dicttable + "') order by -prob limit 20")
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
    plpy.execute('create table madlib.lda_testcorpus ( id int4, contents int4[], topics madlib.topics_t ) distributed randomly')
    plpy.execute('insert into madlib.lda_testcorpus (select * from madlib.mycorpus limit 20)')
    plda_run('madlib.mycorpus', 'madlib.mydict', 'madlib.lda_mymodel', 'madlib.lda_corpus', 30,10,0.5,0.5,False)
    plpy.execute("select madlib.labelTestDocuments('madlib.lda_testcorpus', 'madlib.lda_mymodel', 'madlib.lda_corpus', 'madlib.mydict', 30,10,0.5,0.5)")
    plpy.execute("select * from madlib.lda_testcorpus")


plda_test()


