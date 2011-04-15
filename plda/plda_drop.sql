DROP TYPE IF EXISTS madlib.topics_t CASCADE;
DROP TABLE IF EXISTS madlib.lda_corpus CASCADE;
DROP TABLE IF EXISTS madlib.lda_testcorpus CASCADE;
DROP TABLE IF EXISTS madlib.lda_dict CASCADE; 
DROP TYPE IF EXISTS madlib.global_counts CASCADE;
DROP AGGREGATE IF EXISTS madlib.cword_agg(int4[], int4[], int4, int4, int4);
DROP TYPE IF EXISTS madlib.word_distrn CASCADE;
DROP TYPE IF EXISTS madlib.word_weight CASCADE;

