"""
@file plda.py

@brief Parallel LDA: Driver function
@author Kee Siong Ng

Parallel LDA: Driver function
"""

import plpy

# Each document is represented as an array of integers, with each integer
# representing a word. Word integers must start from 1.

def plda_run(datatable, dicttable, numiter, numtopics, alpha, eta, restart):
    """
    Executes the parallel LDA algorithm.

    @param datatable Name of relation containing the input data points
    @param dicttable Name of relation containing the alphabet dictionary
    @param numiter   Number of iterations to run the Gibbs sampling
    @param numtopics Number of topics to discover
    @param alpha     Parameter of the Dirichlet distribution for document topic mixtures
    @param eta       Parameter of the Dirichlet distribution for per-topic word distributions
    @param restart   This is True if we want to continue from a previously terminated run
    """

    plpy.connect('testdb', 'localhost', 5432, 'gpadmin', 'password')
    plpy.execute('set client_min_messages=info')

    plpy.execute('CREATE TEMP TABLE localWordTopicCount ( id int4, mytimestamp int4, lcounts int4[] ) DISTRIBUTED BY (mytimestamp)');

    restartstep = 0
    if (restart == False):
        plpy.execute("SELECT setseed(0.5)")
        plpy.info('Removing old data from tables')
        # plpy.execute("DELETE FROM madlib.localWordTopicCount")
        # plpy.execute("VACUUM madlib.localWordTopicCount")
        plpy.execute("DELETE FROM madlib.globalWordTopicCount")
        plpy.execute("VACUUM madlib.globalWordTopicCount")
        plpy.execute("DELETE FROM madlib.lda_corpus")
        plpy.execute("VACUUM madlib.lda_corpus")
        plpy.execute("DELETE FROM madlib.lda_dict")
        plpy.execute("VACUUM madlib.lda_dict")
        plpy.info('  .... Done')
        
        plpy.info('Copying training data into tables madlib.lda_corpus and madlib.lda_dict')
        plpy.execute("INSERT INTO madlib.lda_corpus (SELECT id, contents FROM " + datatable + ")")
        plpy.execute("INSERT INTO madlib.lda_dict (SELECT 1000000, dict FROM " + dicttable + " LIMIT 1)")
        plpy.info('  .... Done')

        plpy.info('Assigning initial random topics to documents in the corpus')
        plpy.execute("UPDATE madlib.lda_corpus SET topics = madlib.randomTopics(array_upper(contents,1)," + str(numtopics) + ")")
        plpy.info('  .... Done')
    else:
        rv = plpy.execute("SELECT MAX(mytimestamp) FROM madlib.globalWordTopicCount");
        restartstep = rv[0]['max']
        numiter = numiter - restartstep    

    stepperround = 2
    numrounds = numiter / stepperround
    leftover = numiter % stepperround

    plpy.info('Starting learning process')
    for i in range(0,numrounds):
        plpy.info( 'Starting iteration')
        plpy.execute("select madlib.plda(" + str(numtopics) + "," + str(stepperround) +"," + str(restartstep + i*stepperround) + "," + str(alpha) + "," + str(eta) + ", 'localWordTopicCount')")
        plpy.info( 'Finished iteration %d' % (restartstep + (i+1)*stepperround))
        plpy.execute("VACUUM madlib.lda_corpus")

    if leftover > 0:
        plpy.execute("select madlib.plda(" + str(numtopics) + "," + str(leftover) + "," + str(restartstep + numrounds*stepperround) + "," + str(alpha) + "," + str(eta) + ", 'localWordTopicCount')")

    plpy.info('Finished learning process')     

    plpy.execute('DROP TABLE localWordTopicCount');

    rv = plpy.execute("SELECT MAX(mytimestamp) FROM madlib.globalWordTopicCount");
    finalstep = rv[0]['max']    
    for i in range(1,numtopics+1):
        # rv = plpy.execute("select * from madlib.getImportantWords(" + str(finalstep) + "," + str(i) + "," + str(numtopics) + ") order by -prob")
        rv = plpy.execute("select * from madlib.topicWordProb(" + str(numtopics) + "," + str(i) + "," + str(finalstep) + ") order by -prob limit 20");
        plpy.info( 'Topic %d' % i)
        for j in range(0,min(len(rv),20)):
            word = rv[j]['word']
            prob = rv[j]['prob']
            count = rv[j]['wcount']
            plpy.info( ' %d) %s   \t %f \t %d' % (j+1, word, prob, count));

# Example usage
plda_run('madlib.mycorpus', 'madlib.mydict', 30,10,0.5,0.5,False)

