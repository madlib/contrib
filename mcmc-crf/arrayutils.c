#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "catalog/pg_type.h"

/*
 * return minimum index of n in integer array arr
 */

int32 
minidx_array(ArrayType* arr, int n)
{
	int		ndims;
        int             i;
        int32           minidx;

        ndims = ARR_DIMS(arr)[0];
	minidx = -1;

        for (i=0; i<ndims; i++) {
                if (((int*)ARR_DATA_PTR(arr))[i] == n){
			minidx = i;
			return minidx;
		}
        }

	return minidx;
}

/*
 * return maximum index of n in integer array arr
 */

int32 
maxidx_array(ArrayType* arr, int n)
{
	int		ndims;
        int             i;
        int32           maxidx;

        ndims = ARR_DIMS(arr)[0];
	maxidx = -1;

        for (i=ndims-1; i>=0; i--) {
                if (((int*)ARR_DATA_PTR(arr))[i] == n){
			maxidx = i;
			return maxidx;
		}
        }

	return maxidx;
}

/*
int32 
maxidx_array(ArrayType* arr)
{
        int             ndims;
        int             i;
        int32             maxidx;
        int             max;

        ndims = ARR_DIMS(arr)[0];
        if (ndims == 0)
                return ((int32)0);
        max = ((int*)ARR_DATA_PTR(arr))[0];
	maxidx = 0;

        for (i=1; i<ndims; i++) {
                if (max < ((int*)ARR_DATA_PTR(arr))[i]){
                        max = ((int*)ARR_DATA_PTR(arr))[i];
			maxidx = i;
		}
        }

        return (maxidx);
}
*/


