import plpy

# Each document is represented as an array of integers, with each integer
# representing a word. Word integers must start from 1.

def plda_run(datatable, dicttable, numiter, numtopics, alpha, eta, restart):
    plpy.connect('testdb', 'localhost', 5432, 'gpadmin', 'password')
    # plpy.execute('set client_min_messages=warning')

    restartstep = 0
    if (restart == False):
        plpy.execute("SELECT setseed(0.5)")
        plpy.execute("DELETE FROM madlib.globalWordTopicCount")
        
        plpy.info('Copying data into analysis table madlib.lda_corpus')
        plpy.execute("DELETE FROM madlib.lda_corpus")
        plpy.execute("VACUUM madlib.lda_corpus")
        plpy.execute("INSERT INTO madlib.lda_corpus (SELECT id, contents FROM " + datatable + ")")
        plpy.info('  .... Done')

        plpy.info('Assigning initial random topics to documents in the corpus')
        plpy.execute("UPDATE madlib.lda_corpus SET topics = madlib.randomTopics(array_upper(contents,1)," + str(numtopics) + ")")
        plpy.info('  .... Done')

        plpy.execute("DELETE FROM madlib.lda_dict")
        plpy.execute("VACUUM madlib.lda_dict")
        plpy.execute("INSERT INTO madlib.lda_dict (SELECT 1000000, dict FROM " + dicttable + " LIMIT 1)")
    else:
        rv = plpy.execute("SELECT MAX(mytimestamp) FROM madlib.globalWordTopicCount");
        restartstep = rv[0]['max']
        numiter = numiter - restartstep    

    stepperround = 5
    numrounds = numiter / stepperround
    leftover = numiter % stepperround

    plpy.info('Starting learning process')
    for i in range(0,numrounds):
        plpy.execute("select madlib.plda(" + str(numtopics) + "," + str(stepperround) +"," + str(restartstep + i*stepperround) + "," + str(alpha) + "," + str(eta) + ")")
        plpy.info( 'Finished iteration %d' % (restartstep + (i+1)*stepperround))

    if leftover > 0:
        plpy.execute("select madlib.plda(" + str(numtopics) + "," + str(leftover) + "," + str(restartstep + numrounds*stepperround) + "," + str(alpha) + "," + str(eta) + ")")

    plpy.info('Finished learning process')            

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
# plda_run('madlib.mycorpus', 'madlib.mydict', 50,9,0.5,0.5,False)

