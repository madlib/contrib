PG_CONFIG = pg_config
MODULE_big = viterbi
DATA_built = viterbi.sql sql/enron_test.sql
SRCS = topk_array.c
OBJS = $(SRCS:.c=.o)
SHLIB_LINK = -lpq
REGRESS = enron_test
#DOCS = README.sketch_support

PGXS := $(shell pg_config --pgxs)
include $(PGXS)
include config.mk
