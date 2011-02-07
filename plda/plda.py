import sys
from types import *

try:
    from pygresql import pg
except Exception, e:
    try:
        import pg
    except Exception, e:
        errorMsg = "unable to import The PyGreSQL Python module (pg.py) - %s\n" % str(e)
        sys.stderr.write(str(errorMsg))
        sys.exit(2)
              
# This method establishes the connection to a database.
def connect ( dbname, host, port, user, passwd):
    global db 
    db = pg.DB(  dbname=dbname
               , host=host 
               , port=port
               , user=user
               , passwd=passwd
               );

def close():             
    db.close()
                              
# The following functions should be used inside the user modules
# in order to make their code uniform for both external python scripts 
# or from in-database pl/python functions.   
# ----------                    
def execute( sql):             
    rv = db.query( sql.encode('utf-8'))
    if type(rv) is NoneType:
        return 0
    elif type(rv) is StringType:
        return rv
    else:
        return rv.dictresult()

def info( msg):
        print 'INFO: ' + msg;
        
def error( msg):
        print 'ERROR: ' + msg
        exit( 1) 

# provide way to specify name of table storing the data
# Each document is represented as an array of integers, with each integer
# representing a word. Word integers must start from 1.

def plda_run(numiter, numtopics, alpha, eta, restart):
    connect('testdb', 'localhost', 5432, 'keesiongng', 'lib36514d')
    restartstep = 0
    if (restart == False):
        execute("SELECT setseed(0.5)")
        execute("DELETE FROM madlib.globalWordTopicCount")
        execute("UPDATE madlib.lda_corpus SET topics = madlib.randomTopics(array_upper(contents,1)," + str(numtopics) + ")")
    else:
        rv = execute("SELECT MAX(mytimestamp) FROM madlib.globalWordTopicCount");
        restartstep = rv[0]['max']
        numiter = numiter - restartstep    

    stepperround = 10
    numrounds = numiter / stepperround
    leftover = numiter % stepperround
    for i in range(0,numrounds):
        execute("select madlib.plda(" + str(numtopics) + "," + str(stepperround) +"," + str(restartstep + i*stepperround) + "," + str(alpha) + "," + str(eta) + ")")
    if leftover > 0:
        execute("select madlib.plda(" + str(numtopics) + "," + str(leftover) + "," + str(restartstep + numrounds*stepperround) + "," + str(alpha) + "," + str(eta) + ")")

    rv = execute("SELECT MAX(mytimestamp) FROM madlib.globalWordTopicCount");
    finalstep = rv[0]['max']    
    for i in range(1,numtopics+1):
        rv = execute("select * from madlib.getImportantWords(" + str(finalstep) + "," + str(i) + "," + str(numtopics) + ") order by -prob")
        info( 'Topic %d' % i)
        for j in range(0,min(len(rv),20)):
            word = rv[j]['word']
            prob = rv[j]['prob']
            count = rv[j]['wcount']
            info( ' %d) %s   \t %f \t %d' % (j+1, word, prob, count));

plda_run(100,8,0.5,0.5,False)

