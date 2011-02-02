
#define fetchattref(A,T) fetch_att(T, 0, (A)->attlen)


/****************************************** 
 * nocachegetattrref 
 *******************************************/   

Datum
nocachegetattrref(HeapTuple tuple,
                           int attnum,
                           TupleDesc tupleDesc,
                           bool *isnull)
{
        HeapTupleHeader tup = tuple->t_data;
        Form_pg_attribute *att = tupleDesc->attrs;
        char       *tp;                         /* ptr to data part of tuple */
        bits8      *bp = tup->t_bits;           /* ptr to null bitmap in tuple */
        bool            slow = false;   /* do we have to walk attrs? */
        int                     off;                    /* current offset within data */

        (void) isnull;                          /* not used */

        /* ----------------
 *          *       Three cases:
 *                   *
 *                            *       1: No nulls and no variable-width attributes.
 *                                     *       2: Has a null or a var-width AFTER att.
 *                                              *       3: Has nulls or var-widths BEFORE att.
 *                                                       * ----------------
 *                                                                */

#ifdef IN_MACRO
/* This is handled in the macro */
        Assert(attnum > 0);

        if (isnull)
                *isnull = false;
#endif

        attnum--;

        if (HeapTupleNoNulls(tuple))
        {
#ifdef IN_MACRO
/* This is handled in the macro */
                if (att[attnum]->attcacheoff >= 0)
                {
                        return fetchattref(att[attnum],
 (char *) tup + tup->t_hoff +
                                                        att[attnum]->attcacheoff);
                }
#endif
        }
        else
        {
                /*
 *                  * there's a null somewhere in the tuple
 *                                   *
 *                                                    * check to see if desired att is null
 *                                                                     */

#ifdef IN_MACRO
/* This is handled in the macro */
                if (att_isnull(attnum, bp))
                {
                        if (isnull)
                                *isnull = true;
                        return (Datum) NULL;
                }
#endif

                /*
 *                  * Now check to see if any preceding bits are null...
 *                                   */
                {
                        int byte = attnum >> 3;
                        int                     finalbit = attnum & 0x07;

                        /* check for nulls "before" final bit of last byte */
                        if ((~bp[byte]) & ((1 << finalbit) - 1))
                                slow = true;
                        else
                        {
                                /* check for nulls in any "earlier" bytes */
                                int                     i;

                                for (i = 0; i < byte; i++)
                                {
 if (bp[i] != 0xFF)
                                        {
                                                slow = true;
                                                break;
                                        }
                                }
                        }
                }
        }

        tp = (char *) tup + tup->t_hoff;

        if (!slow)
        {
                /*
 *                  * If we get here, there are no nulls up to and including the target
 *                                   * attribute.  If we have a cached offset, we can use it.
 *                                                    */
                if (att[attnum]->attcacheoff >= 0)
                {
                        return fetchattref(att[attnum],
                                                        tp + att[attnum]->attcacheoff);
                }

                /*
 *                  * Otherwise, check for non-fixed-length attrs up to and including
 *                                   * target.      If there aren't any, it's safe to cheaply initialize the
 *                                                    * cached offsets for these attrs.
 *                                                                     */
                if (HeapTupleHasVarWidth(tuple))
                {
                        int                     j;

                        for (j = 0; j <= attnum; j++)
                        {
                                if (att[j]->attlen <= 0)
                                {
                                        slow = true;
                                        break;
                                }
                        }
                }
        }

        if (!slow)
        {
                int                     natts = tupleDesc->natts;
                int                     j = 1;

                /*
 *                  * If we get here, we have a tuple with no nulls or var-widths up to
 *                                   * and including the target attribute, so we can use the cached offset
 *                                                    * ... only we don't have it yet, or we'd not have got here.  Since
 *                                                                     * it's cheap to compute offsets for fixed-width columns, we take the
 *                                                                                      * opportunity to initialize the cached offsets for *all* the leading
 *                                                                                                       * fixed-width columns, in hope of avoiding future visits to this
 *                                                                                                                        * routine.
 *                                                                                                                                         */
                att[0]->attcacheoff = 0;

                /* we might have set some offsets in the slow path previously */
                while (j < natts && att[j]->attcacheoff > 0)
                        j++;

                off = att[j - 1]->attcacheoff + att[j - 1]->attlen;

                for (; j < natts; j++)
                {
                        if (att[j]->attlen <= 0)
                                break;

                        off = att_align_nominal(off, att[j]->attalign);

                        att[j]->attcacheoff = off;

                        off += att[j]->attlen;
                }

                Assert(j > attnum);
                
                off = att[attnum]->attcacheoff;
        }
        else
        {
                bool            usecache = true;
                int                     i;

                /*
 *                  * Now we know that we have to walk the tuple CAREFULLY.  But we still
 *                                   * might be able to cache some offsets for next time.
 *                                                    *
 *                                                                     * Note - This loop is a little tricky.  For each non-null attribute,
 *                                                                                      * we have to first account for alignment padding before the attr,
 *                                                                                                       * then advance over the attr based on its length.      Nulls have no
 *                                                                                                                        * storage and no alignment padding either.  We can use/set
 *                                                                                                                                         * attcacheoff until we reach either a null or a var-width attribute.
 *                                                                                                                                                          */
                off = 0;
                for (i = 0;; i++)               /* loop exit is at "break" */
                {
                        if (HeapTupleHasNulls(tuple) && att_isnull(i, bp))
                        {
                                usecache = false;
                                continue;               /* this cannot be the target att */
                        }

                        /* If we know the next offset, we can skip the rest */
                        if (usecache && att[i]->attcacheoff >= 0)
                                off = att[i]->attcacheoff;
                        else if (att[i]->attlen == -1)
                        {
                                /*
 * We can only cache the offset for a varlena attribute if the
 *                                  * offset is already suitably aligned, so that there would be
 *                                                                   * no pad bytes in any case: then the offset will be valid for
 *                                                                                                    * either an aligned or unaligned value.
 *                                                                                                                                     */
                                if (usecache &&
                                        off == att_align_nominal(off, att[i]->attalign))
                                        att[i]->attcacheoff = off;
                                else
                                {
                                        off = att_align_pointer(off, att[i]->attalign, -1,
                                                                                        tp + off);
                                        usecache = false;
                                }
                        }
                        else
                        {
                                /* not varlena, so safe to use att_align_nominal */
                                off = att_align_nominal(off, att[i]->attalign);

                                if (usecache)
                                        att[i]->attcacheoff = off;
                        }

                        if (i == attnum)
                                break;

                        off = att_addlength_pointer(off, att[i]->attlen, tp + off);

                        if (usecache && att[i]->attlen <= 0)
                                usecache = false;
                }
        }

        return fetchattref(att[attnum], tp + off);

}


/****************************************** 
 * fastgetattrref 
 *******************************************/   

#define fastgetattrref(tup, attnum, tupleDesc, isnull)                                     \
(                                                                                                                                       \
        AssertMacro((attnum) > 0),                                                                              \
        (((isnull) != NULL) ? (*(isnull) = false) : (dummyret)NULL),    \
        HeapTupleNoNulls(tup) ?                                                                                 \
        (                                                                                                                               \
                (tupleDesc)->attrs[(attnum)-1]->attcacheoff >= 0 ?                      \
                (                                                                                                                       \
                        fetchattref((tupleDesc)->attrs[(attnum)-1],                                \
                                (char *) (tup)->t_data + (tup)->t_data->t_hoff +        \
                                        (tupleDesc)->attrs[(attnum)-1]->attcacheoff)    \
                )                                                                                                                       \
                :                                                                                                                       \
                        nocachegetattrref((tup), (attnum), (tupleDesc), (isnull))  \
        )                                                                                                                               \
        :                                                                                                                               \
        (                                                                                                                               \
att_isnull((attnum)-1, (tup)->t_data->t_bits) ?                         \
                (                                                                                                                       \
                        (((isnull) != NULL) ? (*(isnull) = true) : (dummyret)NULL),             \
                        (Datum)NULL                                                                                             \
                )                                                                                                                       \
                :                                                                                                                       \
                (                                                                                                                       \
                        nocachegetattrref((tup), (attnum), (tupleDesc), (isnull))  \
                )                                                                                                                       \
        )                                                                                                                               \
)




/****************************************** 
 *  heap_getattrref
 *******************************************/   

#define heap_getattrref(tup, attnum, tupleDesc, isnull) \
( \
        AssertMacro((tup) != NULL), \
        ( \
                ((attnum) > 0) ? \
                ( \
                        ((attnum) > (int) HeapTupleHeaderGetNatts((tup)->t_data)) ? \
                        ( \
                                (((isnull) != NULL) ? (*(isnull) = true) : (dummyret)NULL), \
                                (Datum)NULL \
                        ) \
                        : \
                                fastgetattrref((tup), (attnum), (tupleDesc), (isnull)) \
                ) \
                : \
                        heap_getsysattr((tup), (attnum), (tupleDesc), (isnull)) \
        ) \
)

/**************************************
 * GetAttributeBRyRef
 *************************************/

Datum
GetAttributeByRef(HeapTupleHeader tuple,
 const char *attname, bool *isNull)
{
        AttrNumber      attrno;
        Datum           result;
        Oid                     tupType;
        int32           tupTypmod;
        TupleDesc       tupDesc;
        HeapTupleData tmptup;
        int                     i;

        if (attname == NULL)
                elog(ERROR, "invalid attribute name");

        if (isNull == NULL)
                elog(ERROR, "a NULL isNull pointer was passed");

        if (tuple == NULL)
        {
                /* Kinda bogus but compatible with old behavior... */
                *isNull = true;
                return (Datum) 0;
        }

        tupType = HeapTupleHeaderGetTypeId(tuple);
        tupTypmod = HeapTupleHeaderGetTypMod(tuple);
        tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	attrno = InvalidAttrNumber;
        for (i = 0; i < tupDesc->natts; i++)
        {
                if (namestrcmp(&(tupDesc->attrs[i]->attname), attname) == 0)
                {
                        attrno = tupDesc->attrs[i]->attnum;
                        break;
                }
        }

        if (attrno == InvalidAttrNumber)
                elog(ERROR, "attribute \"%s\" does not exist", attname);

        /*
 *          * heap_getattr needs a HeapTuple not a bare HeapTupleHeader.  We set all
 *                   * the fields in the struct just in case user tries to inspect system
 *                            * columns.
 *                                     */
        tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
        ItemPointerSetInvalid(&(tmptup.t_self));
        tmptup.t_tableOid = InvalidOid;
        tmptup.t_data = tuple;

        result = heap_getattrref(&tmptup,
                                                  attrno,
                                                  tupDesc,
                                                  isNull);

        ReleaseTupleDesc(tupDesc);

        return result;
}


