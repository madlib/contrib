PG_CONFIG = pg_config
MODULE_big = mcmccrf
DATA_built = IE_ChainCRF_MCMCMH.sql IE_ChainCRF_MCMCMH_Gibbs.sql
SRCS = arrayutils.c ie_lccrf_mcmcmh.c
OBJS = $(SRCS:.c=.o)
SHLIB_LINK = -lpq

PGXS := $(shell pg_config --pgxs)
include $(PGXS)
include config.mk
