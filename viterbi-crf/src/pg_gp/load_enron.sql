-- Loads enron tables for testing Viterbi CRF

drop schema if exists enron cascade;
create schema enron;
  
-- Factor table
drop table if exists enron.factors_with_dup CASCADE;
create table enron.factors_with_dup (seg_text text, label int, prev_label int, score float);
copy enron.factors_with_dup (seg_text,label,prev_label,score) from '/Users/joeh/Dropbox/madlib/contrib/viterbi-crf/src/pg_gp/enron.test.MR' with CSV;
analyze enron.factors_with_dup;

-- Segment table
drop table if exists enron.segments_with_dup CASCADE;
create table enron.segments_with_dup (doc_id int, seg_id int, start_pos int,end_pos int, seg_text text);
copy enron.segments_with_dup (doc_id,seg_id,start_pos,end_pos,seg_text) 
from '/Users/joeh/Dropbox/madlib/contrib/viterbi-crf/src/pg_gp/enron.test.SegmentTbl' with CSV;
analyze enron.segments_with_dup;
  
-- Labels
drop table if exists enron.labels CASCADE;
create table enron.labels(id integer, label text);
copy enron.labels (id, label) from '/Users/joeh/Dropbox/madlib/contrib/viterbi-crf/src/pg_gp/enron.labels' with CSV;
analyze enron.labels;