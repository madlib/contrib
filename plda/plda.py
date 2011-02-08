import plpy

# provide way to specify name of table storing the data
# Each document is represented as an array of integers, with each integer
# representing a word. Word integers must start from 1.

def plda_run(numiter, numtopics, alpha, eta, restart):
    plpy.connect('testdb', 'localhost', 5432, 'keesiongng', 'lib36514d')
    restartstep = 0
    if (restart == False):
        plpy.execute("SELECT setseed(0.5)")
        plpy.execute("DELETE FROM madlib.globalWordTopicCount")
        plpy.execute("UPDATE madlib.lda_corpus SET topics = madlib.randomTopics(array_upper(contents,1)," + str(numtopics) + ")")
    else:
        rv = plpy.execute("SELECT MAX(mytimestamp) FROM madlib.globalWordTopicCount");
        restartstep = rv[0]['max']
        numiter = numiter - restartstep    

    stepperround = 10
    numrounds = numiter / stepperround
    leftover = numiter % stepperround
    for i in range(0,numrounds):
        plpy.execute("select madlib.plda(" + str(numtopics) + "," + str(stepperround) +"," + str(restartstep + i*stepperround) + "," + str(alpha) + "," + str(eta) + ")")
    if leftover > 0:
        plpy.execute("select madlib.plda(" + str(numtopics) + "," + str(leftover) + "," + str(restartstep + numrounds*stepperround) + "," + str(alpha) + "," + str(eta) + ")")

    rv = plpy.execute("SELECT MAX(mytimestamp) FROM madlib.globalWordTopicCount");
    finalstep = rv[0]['max']    
    for i in range(1,numtopics+1):
        rv = plpy.execute("select * from madlib.getImportantWords(" + str(finalstep) + "," + str(i) + "," + str(numtopics) + ") order by -prob")
        plpy.info( 'Topic %d' % i)
        for j in range(0,min(len(rv),20)):
            word = rv[j]['word']
            prob = rv[j]['prob']
            count = rv[j]['wcount']
            plpy.info( ' %d) %s   \t %f \t %d' % (j+1, word, prob, count));

# plda_run(20,10,0.5,0.5,False)

