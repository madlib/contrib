\timing

/*
 * Creates a randomly generated graph in the form of triple stores (v1,v2,value).
 * Edges are assumed to be directed; an undirected edge with weight w between two vertices 
 * v1 and v2 is represented as two edges (v1,v2,w) and (v2,v1,w).
 * Duplicate edges can be generated.
 */
CREATE OR REPLACE FUNCTION madlib.randomGraph(graph_table text, gsize int, esize int) RETURNS VOID AS $$
       plpy.execute("DROP TABLE IF EXISTS " + graph_table)
       plpy.execute("CREATE TABLE " + graph_table + " (v1 int, v2 int, weight float) " +
       		    "DISTRIBUTED BY (v1,v2)")
       import random
       random.seed(10)
       for i in range(1,esize):
       	   v1 = random.randint(1,gsize-1)
       	   v2 = random.randint(v1+1,gsize)
       	   plpy.execute("INSERT INTO " + graph_table + 
	   		" VALUES (" + str(v1) + "," + str(v2) + ", 1), " + 
			"        (" + str(v2) + "," + str(v1) + ", 1)")
$$ LANGUAGE plpythonu;

DROP TYPE IF EXISTS madlib.mcstate CASCADE;
CREATE TYPE madlib.mcstate AS ( minval float, vcount int );

/*
 * This is the state transition function for madlib.mymincount, which returns the total number of paths with the
 * smallest distance between any two vertices.
 */
CREATE OR REPLACE FUNCTION madlib.mymincount_sfunc(st madlib.mcstate, val float, paths int) 
RETURNS madlib.mcstate AS $$
DECLARE
BEGIN
	IF st.minval < 0 OR val < st.minval THEN
	   st.minval := val;
	   st.vcount := paths;
	ELSIF val = st.minval THEN
	   st.vcount := st.vcount + paths;
	END IF;
	RETURN st;
END;
$$ LANGUAGE plpgsql;

-- DROP AGGREGATE IF EXISTS madlib.mymincount(float, int);
CREATE AGGREGATE madlib.mymincount(float, int) (
       sfunc = madlib.mymincount_sfunc,
       stype = madlib.mcstate,
       initcond = '(-5,0)'
);

/*
 * Inserts elements from the second array into the first, making sure to remove duplicates.
 * The first array is assumed to contain no duplicates.
 */
CREATE OR REPLACE FUNCTION madlib.insertArray(arr1 int[], arr2 int[]) RETURNS int[] AS $$
DECLARE
	ret int[];
	n2 int;
	n1 int;
	seen boolean;
BEGIN
	n1 := array_upper(arr1,1);
	n2 := array_upper(arr2,1);
	ret := arr1;
	FOR i IN 1..n2 LOOP
	    seen := false;
	    FOR j IN 1..n1 LOOP
	    	IF arr2[i] = ret[j] THEN
		   seen := true;
		   EXIT;
		END IF;
	    END LOOP;
	    IF seen = false THEN
	       ret := array_append(ret,arr2[i]);
	       n1 := n1 + 1;
	    END IF;
	END LOOP;
	RETURN ret;
END;
$$ LANGUAGE plpgsql;

DROP TYPE IF EXISTS madlib.mcstate2 CASCADE;
CREATE TYPE madlib.mcstate2 AS ( minval float, parents int[] );

/*
 * This is the state transition function for madlib.mymincount2, which collects all the parents 
 * associated with shortest paths between any two vertices.
 */
CREATE OR REPLACE FUNCTION madlib.mymincount_sfunc2(st madlib.mcstate2, val float, parent int[]) 
RETURNS madlib.mcstate2 AS $$
DECLARE
BEGIN
	IF st.minval < 0 OR val < st.minval THEN
	   st.minval := val;
	   st.parents := parent;
	ELSIF val = st.minval THEN
	   st.parents := madlib.insertArray(st.parents, parent);
	END IF;
	RETURN st;
END;
$$ LANGUAGE plpgsql;

-- DROP AGGREGATE IF EXISTS madlib.mymincount2(float, int[]);
CREATE AGGREGATE madlib.mymincount2(float, int[]) (
       sfunc = madlib.mymincount_sfunc2,
       stype = madlib.mcstate2,
       initcond = '(-5,{})'
);

/*
 * Returns an array of zeros
 */
create or replace function zeros(n int) returns int[] as $$ 
declare
	ret int[];
begin
	for i in 1..n loop
	    ret[i] = 0;
	end loop;
	return ret;
end;
$$ language plpgsql;

/*
 * Computes the shortest paths between all pairs of vertices in a given graph.
 */
CREATE OR REPLACE FUNCTION madlib.shortestPaths(graph_table text, result_table text) RETURNS VOID AS $$
       # Create a local copy of the graph table that aggregates the weights of duplicate edges 
       plpy.execute("CREATE TEMP TABLE G ( v1 int, v2 int, weight float ) DISTRIBUTED BY (v1,v2)")
       plpy.execute("INSERT INTO G SELECT v1,v2,1.0/sum(weight) FROM " + graph_table + " GROUP BY v1,v2")

       #
       # -- We use an alternating series of temp tables to store the temporary results, starting with R0 
       # -- paths is the number of shortest paths connecting v1 and v2 computed so far.
       # -- parents is the set of the parents of v2 on the shortest paths connecting v1 and v2. 
       # -- parents is needed by the Brandes algorithm for computing betweenness centrality scores.
       #
       plpy.execute("CREATE TEMP TABLE " + 
       		    "R0 (v1 int, v2 int, distance float, paths int, parents int[], timestep int) " +
       		    "WITH (appendonly=true, orientation=column, compresstype=quicklz) DISTRIBUTED BY (v1)")
       plpy.execute("INSERT INTO R0 SELECT v1,v2,weight,1,array[v1],1 FROM G")

       new_table = "R0"

       #
       # -- The algorithm works by iteratively expanding the fringes of the search graph looking for shortest
       # -- paths. This is done by joining the current computed shortest paths in R(0|1) with G to compute the 
       # -- next set of shortest paths.
       #
       for i in range(1,6):

       	   # -- compute table ids 
           new_table_id = i % 2
           if (new_table_id == 0):
                old_table_id = 1
           else:
                old_table_id = 0

           old_table = "R" + str(old_table_id)
           new_table = "R" + str(new_table_id)

      	   icount_t = plpy.execute("SELECT COUNT(*) c FROM " + old_table)
	   icount = icount_t[0]['c']

	   # -- Add the paths that can be obtained by extending an existing shortest path by one edge 
       	   plpy.execute("INSERT INTO " + old_table + " " +
	   		"SELECT " + old_table + ".v1, G.v2, distance + G.weight, paths, array[G.v1], timestep+1 " +
			"  FROM " + old_table + ", G " +
			" WHERE timestep = " + str(i) +
			"   AND " + old_table + ".v2 = G.v1 " +
			"   AND " + old_table + ".v1 <> G.v2")

	   plpy.execute("CREATE TEMP TABLE " + new_table + 
	   		"           (v1 int, v2 int, distance float, paths int, parents int[], timestep int )" +
	   		"WITH (appendonly=true, orientation=column, compresstype=quicklz) DISTRIBUTED BY (v1)")

	   # 
	   # -- Here we compute the shortest paths that can be achieved for each number of edges.
	   # -- Note: The shortest paths are calculated based on the weights on the edges, not 
	   # -- the number of edges. We need to calculate the intermediate shortest paths for each
	   # -- number of edges to avoid double counting shortest paths.
	   # -- Note: We can store only the intermediate shortest paths because every shortest path 
	   # -- is by necessity constructed by extension from another shortest path.
	   #
	   plpy.execute("INSERT INTO " + new_table + " " +
	   		"SELECT v1,v2,min(distance), (madlib.mymincount(distance,paths)).vcount, " +
			"       (madlib.mymincount2(distance,parents)).parents, timestep " +
			"  FROM " + old_table + " " +
			"GROUP BY v1,v2,timestep")

	   plpy.execute("DROP TABLE " + old_table)

       	   fcount_t = plpy.execute("SELECT COUNT(*) c FROM " + new_table)
	   fcount = fcount_t[0]['c']

	   plpy.info(' iteration %d, size %d, increment %d' % (i,fcount,fcount-icount))	   
	   if (icount == fcount): 
	      plpy.info(' icount %d fcount %d' % (icount,fcount))	   
	      break

       #	      
       # -- As a final step, we compute the shortest paths from the intermediate result of computed
       # -- shortest paths for each number of edges.
       #	      
       plpy.execute("DROP TABLE IF EXISTS " + result_table)
       plpy.execute("CREATE TABLE " + result_table + 
       		    "      (v1 int, v2 int, distance float, paths int, parents int[] ) " + 
		    "DISTRIBUTED BY (v1,v2)")
       plpy.execute("INSERT INTO " + result_table + 
       		    " SELECT v1,v2,min(distance), (madlib.mymincount(distance,paths)).vcount," + 
       	 	    "        (madlib.mymincount2(distance,parents)).parents FROM " + new_table + 
		    " GROUP BY v1,v2")

       plpy.execute("DROP TABLE " + new_table)
       plpy.execute("DROP TABLE G")

$$ LANGUAGE plpythonu;

DROP TYPE IF EXISTS madlib.vertex_bcentrality CASCADE;
CREATE TYPE madlib.vertex_bcentrality AS ( vertex int, bcentrality float );

/*
 * This function implements Brandes' algorithm for computing betweenness centrality scores.
 * This function is not parallelised.
 */
CREATE OR REPLACE FUNCTION madlib.btwn_centrality(spaths_table text, vertices text) 
RETURNS SETOF madlib.vertex_bcentrality AS $$

       vtcs_t = plpy.execute("SELECT * FROM " + vertices);
       nvertices = vtcs_t.nrows() 

       # -- Initialise the cb and delta arrays
       cb_t = plpy.execute("SELECT * FROM zeros(" + str(nvertices+1) + ")")
       cb = cb_t[0]['zeros']
       cb = map(float, cb[1:-1].split(','))

       delta_t = plpy.execute("SELECT * FROM zeros(" + str(nvertices+1) + ")")
       delta = delta_t[0]['zeros']
       delta = map(float, delta[1:-1].split(','))

       for i in range(0,nvertices):

       	   s = vtcs_t[i]['v'];

           # -- delta[n] = \sum_t d_st(n) = \sum_t \sigma_st(n) / \sigma_st
       	   for n in range(1,nvertices+1):
	       delta[n] = 0

	   # -- Get all the paths from s to some vertex sorted by non-increasing distance from s    
	   vertex_paths_t = plpy.execute("SELECT v2, distance, paths, parents " + 
	   		    		 " FROM " + spaths_table + 
					 " WHERE v1 = " + str(s) +
					 " ORDER BY -distance")

	   # -- if (vertex_paths_t.nrows() > 0):				 
	   # --    plpy.info('processing s = %d #w = %d' % (s,vertex_paths_t.nrows()))

	   for j in range(0,vertex_paths_t.nrows()):
	       w = vertex_paths_t[j]['v2']
	       # plpy.info('w = %d' % w)
	       
	       dist = vertex_paths_t[j]['distance']

	       sigma_w = vertex_paths_t[j]['paths']
	       v_parents = vertex_paths_t[j]['parents']
	       v_parents = map(int, v_parents[1:-1].split(','))  # -- convert strings into arrays

	       # -- Do an update to delta for each path s ~> v -> w 				 
	       for k in range(0,len(v_parents)):
	       	   v = v_parents[k]
		   # plpy.info('v = %d' % v)

		   # -- Get number of paths from s to v
		   sigma_v_t = plpy.execute("SELECT paths FROM " + spaths_table + 
		   	       		    " WHERE v1 = " + str(s) + " AND v2 = " + str(v))
		   if (sigma_v_t.nrows() == 0):
		      continue			    
		   sigma_v = sigma_v_t[0]['paths']
		   delta_v = (sigma_v * 1.0 / sigma_w) * (1.0 + delta[w])
	       	   delta[v] = delta[v] + delta_v
		   # -- plpy.info('s = %d v = %d w = %d delta[%d] = %f delta[%d] = %f' % (s,v,w,v,delta[v],w,delta[w]))

	       cb[w] = cb[w] + delta[w]
	       # -- plpy.info('old s = %d inc bc[%d] = %f' % (s,w,delta[w]))

       ret = []
       for i in range(0,nvertices):
       	   s = vtcs_t[i]['v']
	   if (cb[s] == 0):
	      continue
       	   ret = ret + [(s,cb[s]/2.0)]
       return ret
$$ LANGUAGE plpythonu;

/*
 * This is the state for the aggregate function that computes the betweenness centrality scores
 */
DROP TYPE IF EXISTS madlib.bc_state CASCADE;
CREATE TYPE madlib.bc_state AS ( last_source int, last_w int, delta float[], bc float[] );

/*
 * This is the state-transition function for the aggregate function that computes the betweenness 
 * centrality scores.
 * The shortest paths originating from each vertex s are all located in the same segment and we
 * compute the delta_s*(v) score for each such s.
 */
CREATE OR REPLACE FUNCTION 
madlib.bc_sf(st madlib.bc_state, s int, w int, distance float, paths_sw int, v int, paths_sv int, nvertices int)
RETURNS madlib.bc_state AS $$
DECLARE
BEGIN
	/* 
	 * Shortest paths take the form of s ~> v -> w, with paths_sw such paths and paths_sv shortest
	 * paths from s to v. 
	 * For each such path s ~> v -> w, we update delta[v].
	 * For a given s, it is assumed that the shortest paths are listed in non-increasing order on
	 * distance. The correctness of the Brandes algorithm relies on that.
	 */

	IF st.last_source = -5 THEN

	   /* first call to state-transition function; perform initialisation */
	   st.last_source := s;
	   st.last_w := w;
	   st.delta := zeros(nvertices);
	   st.bc := zeros(nvertices);

	ELSEIF st.last_source = s AND st.last_w <> w THEN

	   /* same s, but w changed, need to update st.bc */
	   st.bc[st.last_w] := st.bc[st.last_w] + st.delta[st.last_w];
	   st.last_w := w;

	ELSEIF st.last_source <> s THEN

	   /* different s, need to update st.bc and zero out st.delta */
	   st.bc[st.last_w] := st.bc[st.last_w] + st.delta[st.last_w];
	   st.last_source = s;
	   st.last_w = w;
	   FOR i IN 1..nvertices LOOP
	       st.delta[i] := 0;
	   END LOOP;

	END IF;

	/* This update formula comes from Theorem 6 in the Brandes paper */
	st.delta[v] := st.delta[v] + (paths_sv * 1.0 / paths_sw) * (1.0 + st.delta[w]);
	-- RAISE NOTICE 's = % v = % w = % delta[%] = % delta[%] = %', s,v,w,v,st.delta[v],w,st.delta[w];
	RETURN st;
END;
$$ LANGUAGE plpgsql;

/*
 * This is the final function for the bc_agg aggregate function.
 */
CREATE OR REPLACE FUNCTION madlib.bc_ff(st madlib.bc_state) RETURNS madlib.bc_state AS $$
DECLARE
BEGIN
	st.bc[st.last_w] := st.bc[st.last_w] + st.delta[st.last_w];
	-- RAISE NOTICE 'new s = % inc bc[%] = %', st.last_source, st.last_w, st.delta[st.last_w];
	RETURN st;
END;
$$ LANGUAGE plpgsql;

DROP AGGREGATE IF EXISTS madlib.bc_agg(int,int,float,int,int,int,int);
CREATE AGGREGATE madlib.bc_agg(int, int, float, int, int, int, int) (
       sfunc = madlib.bc_sf,
       stype = madlib.bc_state,
       finalfunc = madlib.bc_ff,
       initcond = '(-5,-5,{},{})'
);

/*
 * This function implements Brandes' algorithm for computing betweenness centrality scores.
 */
CREATE OR REPLACE FUNCTION madlib.btwn_centrality_par(spaths_table text, vertices text) 
RETURNS SETOF madlib.vertex_bcentrality AS $$

	vtcs_t = plpy.execute("SELECT * FROM " + vertices);
	nvertices = vtcs_t.nrows() 

	# -- First flatten out the parents column of the input spaths_table
	plpy.execute("CREATE TEMP TABLE spaths (v1 int, v2 int, distance float, paths int, parent int) " +
		     "DISTRIBUTED BY (v1)")
	plpy.execute("INSERT INTO spaths SELECT v1, v2, distance, paths, unnest(parents) FROM " + spaths_table)
	# -- These self paths are needed to make sure edges from the original graph are included in 
	# -- spaths2 (see below); are these edges really needed? 
	plpy.execute("INSERT INTO spaths SELECT ss,ss,0,1,-5  FROM generate_series(1," + str(nvertices) + ") ss")

	# -- The table spaths2 is like spaths, but with the number of paths from v1 to parent calculated
	plpy.execute("CREATE TEMP TABLE " +
		     "   spaths2 (v1 int, v2 int, distance float, paths int, parent int, paths_parent int) " +
		     "DISTRIBUTED BY (v1)")
	plpy.execute("INSERT INTO spaths2 " + 
		     "SELECT R1.v1, R1.v2, R1.distance, R1.paths, R1.parent, R2.paths " +
		     "  FROM spaths R1, spaths R2 " +
		     " WHERE R2.v1 = R1.v1 AND R2.v2 = R1.parent")

	# -- Compute the betweenness centrality scores for the vertices in parallel; all paths originating
	# -- from the same source must reside on the same segment for this parallel algorithm to be correct
	plpy.execute("CREATE TEMP TABLE inter_result ( id int, bc float[] )")
	plpy.execute("INSERT INTO inter_result " +
		     "SELECT gp_segment_id, " + 
		     "    (madlib.bc_agg(v1,v2,distance,paths,parent,paths_parent," + str(nvertices) + ")).bc " +
		     "  FROM (SELECT gp_segment_id, * FROM spaths2 ORDER BY v1,-distance,v2) tempR " +
		     "GROUP BY gp_segment_id")

	# -- Sum up the intermediate results computed on each segment
	finalbc_t = plpy.execute("SELECT sum(bc) bc FROM inter_result")
	finalbc = finalbc_t[0]['bc']
	finalbc = map(float, finalbc[1:-1].split(','))

	# -- Return vertices with non-zero betweenness centrality scores
	ret = []
	for i in range(0,nvertices):
	    if (finalbc[i] == 0):
	       continue
	    ret = ret + [(i+1,finalbc[i]/2.0)]

	return ret
$$ LANGUAGE plpythonu;



SELECT madlib.randomGraph('madlib.mygraph', 2000, 2000);
-- SELECT v1,v2,1.0/sum(weight) FROM madlib.mygraph GROUP BY v1,v2 ORDER BY v1,v2;

SELECT madlib.shortestPaths('madlib.mygraph', 'madlib.myresult');
-- SELECT gp_segment_id, * FROM madlib.myresult ORDER BY gp_segment_id,v1,v2 ;


DROP TABLE IF EXISTS vertices;
CREATE TABLE vertices ( v int ) DISTRIBUTED BY (v);
INSERT INTO vertices (SELECT * FROM generate_series(1,2000));

SELECT * FROM madlib.btwn_centrality_par('madlib.myresult', 'vertices') ORDER BY bcentrality,vertex LIMIT 10;
-- SELECT * FROM madlib.btwn_centrality('madlib.myresult', 'vertices') ORDER BY vertex;


/*
SELECT * FROM spaths;
SELECT * FROM spaths2;
SELECT id, bc[1:10] FROM inter_result;
*/

/*
-- The following is a brute-force way of computing betweenness centrality, done by joining
-- multiple tables together. This is useful for correctness checking on small graphs.

DROP TABLE IF EXISTS centrality;
CREATE TABLE centrality ( v int, btw_centr float ) DISTRIBUTED BY (v);

CREATE TEMP TABLE c_temp ( v int, btw_centr float ) DISTRIBUTED BY (v);

DROP TABLE IF EXISTS critical_paths;
CREATE TABLE critical_paths ( s int, v int, t int, dsv float, psv int, dvt float, pvt int, dst float, pst int)
DISTRIBUTED BY (v);

INSERT INTO critical_paths
SELECT V1.v, V2.v, V3.v, R1.distance, R1.paths, R2.distance, R2.paths, R3.distance, R3.paths
  FROM vertices V1, vertices V2, vertices V3, madlib.myresult R1, madlib.myresult R2, madlib.myresult R3
 WHERE V1.v < V2.v AND V2.v < V3.v 
   AND V1.v = R1.v1 AND V2.v = R1.v2
   AND V2.v = R2.v1 AND V3.v = R2.v2
   AND V1.v = R3.v1 AND V3.v = R3.v2
   AND R1.distance + R2.distance = R3.distance
ORDER BY V2.v;


INSERT INTO c_temp
SELECT V2.v, SUM(R1.paths::float * R2.paths::float / R3.paths::float)
  FROM vertices V1, vertices V2, vertices V3, madlib.myresult R1, madlib.myresult R2, madlib.myresult R3
 WHERE V1.v < V2.v AND V2.v < V3.v 
   AND V1.v = R1.v1 AND V2.v = R1.v2
   AND V2.v = R2.v1 AND V3.v = R2.v2
   AND V1.v = R3.v1 AND V3.v = R3.v2
   AND R1.distance + R2.distance = R3.distance
GROUP BY V2.v;


INSERT INTO critical_paths
SELECT V1.v, V2.v, V3.v, R1.distance, R1.paths, R2.distance, R2.paths, R3.distance, R3.paths
  FROM vertices V1, vertices V2, vertices V3, madlib.myresult R1, madlib.myresult R2, madlib.myresult R3
 WHERE V1.v < V3.v AND V2.v < V1.v
   AND V2.v = R1.v1 AND V1.v = R1.v2
   AND V2.v = R2.v1 AND V3.v = R2.v2
   AND V1.v = R3.v1 AND V3.v = R3.v2
   AND R1.distance + R2.distance = R3.distance
ORDER BY V2.v;


INSERT INTO c_temp
SELECT V2.v, SUM(R1.paths::float * R2.paths::float / R3.paths::float)
  FROM vertices V1, vertices V2, vertices V3, madlib.myresult R1, madlib.myresult R2, madlib.myresult R3
 WHERE V1.v < V3.v AND V2.v < V1.v
   AND V2.v = R1.v1 AND V1.v = R1.v2
   AND V2.v = R2.v1 AND V3.v = R2.v2
   AND V1.v = R3.v1 AND V3.v = R3.v2
   AND R1.distance + R2.distance = R3.distance
GROUP BY V2.v;


INSERT INTO critical_paths
SELECT V1.v, V2.v, V3.v, R1.distance, R1.paths, R2.distance, R2.paths, R3.distance, R3.paths
  FROM vertices V1, vertices V2, vertices V3, madlib.myresult R1, madlib.myresult R2, madlib.myresult R3
 WHERE V1.v < V3.v AND V2.v > V3.v
   AND V1.v = R1.v1 AND V2.v = R1.v2
   AND V3.v = R2.v1 AND V2.v = R2.v2
   AND V1.v = R3.v1 AND V3.v = R3.v2
   AND R1.distance + R2.distance = R3.distance
ORDER BY V2.v;


INSERT INTO c_temp
SELECT V2.v, SUM(R1.paths::float * R2.paths::float / R3.paths::float)
  FROM vertices V1, vertices V2, vertices V3, madlib.myresult R1, madlib.myresult R2, madlib.myresult R3
 WHERE V1.v < V3.v AND V2.v > V3.v
   AND V1.v = R1.v1 AND V2.v = R1.v2
   AND V3.v = R2.v1 AND V2.v = R2.v2
   AND V1.v = R3.v1 AND V3.v = R3.v2
   AND R1.distance + R2.distance = R3.distance
GROUP BY V2.v;

INSERT INTO centrality
SELECT v, sum(btw_centr)
  FROM c_temp
GROUP BY v;

SELECT * FROM centrality ORDER BY v;
*/

/*
CREATE OR REPLACE FUNCTION madlib.shortestPaths_old(graph_table text, result_table text) RETURNS VOID AS $$
       plpy.execute("CREATE TEMP TABLE G ( v1 int, v2 int, weight float ) DISTRIBUTED BY (v1,v2)")
       plpy.execute("INSERT INTO G SELECT * FROM " + graph_table + " WHERE v1 < v2")
       plpy.execute("CREATE TEMP TABLE R0 ( v1 int, v2 int, distance float, paths int, parents int[] ) " +
       		    "WITH (appendonly=true, orientation=column, compresstype=quicklz) DISTRIBUTED BY (v1)")
       plpy.execute("INSERT INTO R0 SELECT v1,v2,weight,1,array[v1] FROM G")

       for i in range(1,20):

           new_table_id = i % 2
           if (new_table_id == 0):
                old_table_id = 1
           else:
                old_table_id = 0

           old_table = "R" + str(old_table_id)
           new_table = "R" + str(new_table_id)

      	   icount_t = plpy.execute("SELECT COUNT(*) c FROM " + old_table)
	   icount = icount_t[0]['c']

       	   plpy.execute("INSERT INTO " + old_table + " " +
	   		"SELECT " + old_table + ".v1, G.v2, distance+1, paths, array[G.v1] " +
			"  FROM " + old_table + ", G " +
			" WHERE distance = " + str(i) +
			"   AND " + old_table + ".v2 = G.v1 ")

	   plpy.execute("INSERT INTO " + old_table + " " +
	   		"SELECT " + old_table + ".v2, G.v2, distance+1, paths, array[G.v1] " +
			"  FROM " + old_table + ", G " +
			" WHERE distance = " + str(i) +
			"   AND " + old_table + ".v1 = G.v1 " +
			"   AND " + old_table + ".v2 < G.v2 ")

	   plpy.execute("INSERT INTO " + old_table + " " +
	   		"SELECT " + old_table + ".v1, G.v1, distance+1, paths, array[G.v2] " +
			"  FROM " + old_table + ", G " +
			" WHERE distance = " + str(i) +
			"   AND " + old_table + ".v2 = G.v2 " +
			"   AND " + old_table + ".v1 < G.v1 ")

	   plpy.execute("CREATE TEMP TABLE " + new_table + " ( v1 int, v2 int, distance float, paths int, parents int[] )" +
	   		"WITH (appendonly=true, orientation=column, compresstype=quicklz) DISTRIBUTED BY (v1)")

	   plpy.execute("INSERT INTO " + new_table + " " +
	   		"SELECT v1,v2,min(distance), (madlib.mymincount(distance,paths)).vcount, " +
			"       (madlib.mymincount2(distance,parents)).parents " +
			"  FROM " + old_table + " " +
			"GROUP BY v1,v2")

	   plpy.execute("DROP TABLE " + old_table)

       	   fcount_t = plpy.execute("SELECT COUNT(*) c FROM " + new_table)
	   fcount = fcount_t[0]['c']

	   plpy.info(' iteration %d, size %d' % (i,fcount))	   
	   if (icount == fcount): 
	      plpy.info(' icount %d fcount %d' % (icount,fcount))	   
	      break

       plpy.execute("INSERT INTO " + result_table + " SELECT v1,v2,distance,paths,parents FROM " + new_table)
       plpy.execute("DROP TABLE " + new_table)

$$ LANGUAGE plpythonu;
*/

