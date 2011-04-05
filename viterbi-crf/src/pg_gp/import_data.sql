
-----------------------------------------------------
-- To use for a different dataset:
-- 1. replace the location of the vcrf_factors table file
-- 2. replace the location of the vcrf_segments file
-----------------------------------------------------

-- set up scratch tables
drop table if exists madlib.vcrf_factors_with_dup;
create table madlib.vcrf_factors_with_dup (seg_text text, label int, prev_label int, score float);

drop table if exists madlib.vcrf_factorstable;
create table madlib.vcrf_factorstable (seg_id int, label int, prev_label int, score int);

drop table if exists madlib.vcrf_segments_with_dup;
create table madlib.vcrf_segments_with_dup (doc_id int, seg_id int, start_pos int,end_pos int, seg_text text);

truncate madlib.vcrf_factors_with_dup;
copy madlib.vcrf_factors_with_dup (seg_text,label,prev_label,score) from '/Users/joeh/Dropbox/madlib/contrib/viterbi-crf/src/pg_gp/enron.test.MR' with CSV;
analyze madlib.vcrf_factors_with_dup;

truncate madlib.vcrf_labels;
copy madlib.vcrf_labels (id, label) from '/Users/joeh/Dropbox/madlib/contrib/viterbi-crf/src/pg_gp/enron.labels' with CSV;
analyze madlib.vcrf_labels;

truncate madlib.vcrf_segments_with_dup;
copy madlib.vcrf_segments_with_dup (doc_id,seg_id,start_pos,end_pos,seg_text) 
from '/Users/joeh/Dropbox/madlib/contrib/viterbi-crf/src/pg_gp/enron.test.SegmentTbl' with CSV;
analyze madlib.vcrf_segments_with_dup;

truncate madlib.vcrf_segment_lookup;
insert into madlib.vcrf_segment_lookup select seg_text, rank() over (order by seg_text) from (select distinct(seg_text) from madlib.vcrf_segments_with_dup) as A;
analyze madlib.vcrf_segment_lookup;

truncate madlib.vcrf_segments;
insert into madlib.vcrf_segments select doc_id, start_pos, SH.seg_id from madlib.vcrf_segments_with_dup S, madlib.vcrf_segment_lookup SH where S.seg_text=SH.seg_text order by doc_id, start_pos;
analyze madlib.vcrf_segments;

truncate madlib.vcrf_factorstable;
insert into madlib.vcrf_factorstable select seg_id,label,prev_label,(max(score)*1000) as score from madlib.vcrf_factors_with_dup factors, madlib.vcrf_segment_lookup SH where factors.seg_text=SH.seg_text group by seg_id,label,prev_label;
analyze madlib.vcrf_factorstable;

-- array representation of the factors table
truncate madlib.vcrf_factors;
insert into madlib.vcrf_factors select seg_id, pg_catalog.array_agg(score) from (select seg_id, score from madlib.vcrf_factorstable order by seg_id, prev_label, label) as ss group by seg_id order by seg_id;
analyze madlib.vcrf_factors;

truncate madlib.vcrf_doc_ids;
insert into madlib.vcrf_doc_ids select distinct(doc_id) from madlib.vcrf_segments order by doc_id;
analyze madlib.vcrf_doc_ids;

truncate madlib.vcrf_norm_factors;
insert into madlib.vcrf_norm_factors select doc_id, madlib.vcrf_normalization('madlib.vcrf_segments', 'madlib.vcrf_factors', doc_id) from madlib.vcrf_doc_ids;

drop table madlib.vcrf_factors_with_dup;
drop table madlib.vcrf_segments_with_dup;
drop table madlib.vcrf_factorstable;



