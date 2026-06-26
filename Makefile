# Test build for rayforce-q.
#
# Builds a small rayforce-linked binary that registers the Q client as
# rayfall builtins, then runs the .rfl tests against a live q server. The
# native rayforce engine is cloned (or copied from a local checkout) and built
# as a static lib that this driver links against.
#
#   make test                                  # clone core, build, run
#   RAYFORCE_LOCAL_PATH=/path/to/core make test  # use a local core checkout
#
# Variables:
#   RAYFORCE_GITHUB      core git URL (default: public repo)
#   RAYFORCE_REF         branch/tag to check out (optional)
#   RAYFORCE_LOCAL_PATH  rsync this checkout instead of cloning
#   Q_BINARY             path to the q binary (run_tests.sh autodetects)
#   PORT                 q listen port (run_tests.sh picks a free one if unset)

UNAME_S := $(shell uname -s)
ROOT    := $(shell pwd)
CORE    := $(ROOT)/test/tmp/rayforce-c

RAYFORCE_GITHUB ?= https://github.com/RayforceDB/rayforce.git
RAYFORCE_REF ?=
RAYFORCE_LOCAL_PATH ?=

CC ?= cc
CFLAGS  = -std=c17 -O2 -fPIC -I$(ROOT) -I$(CORE)/include -I$(CORE)/src \
          -Wno-unused-function
ifeq ($(UNAME_S),Linux)
  LIBS = -lm -lpthread
else
  LIBS = -lm
endif

SRCS = q.c test/q_builtins.c test/q_test.c

pull_core:
	@rm -rf $(CORE)
	@mkdir -p $(ROOT)/test/tmp
	@if [ -n "$(RAYFORCE_LOCAL_PATH)" ]; then \
		echo "📂 Copying rayforce core from $(RAYFORCE_LOCAL_PATH)..."; \
		rsync -a --exclude='.git' --exclude='tmp' --exclude='build*' \
			--exclude='*.o' --exclude='*.so' --exclude='*.a' --exclude='*.dylib' \
			"$(RAYFORCE_LOCAL_PATH)/" "$(CORE)/"; \
	else \
		echo "⬇️  Cloning rayforce core from $(RAYFORCE_GITHUB)..."; \
		git clone $(if $(RAYFORCE_REF),--branch $(RAYFORCE_REF)) \
			--depth 1 $(RAYFORCE_GITHUB) "$(CORE)"; \
	fi

core_lib: pull_core
	@echo "🔨 Building librayforce.a..."
	@$(MAKE) -C $(CORE) lib

q_test: core_lib
	@echo "🔧 Building q_test driver..."
	@$(CC) $(CFLAGS) -o test/q_test $(SRCS) $(CORE)/librayforce.a $(LIBS)

lint:
	clang-format -i ./q.c ./q.h ./test/q_builtins.c ./test/q_test.c

test: q_test
	@./test/run_tests.sh ./test/q_test test/rfl

clean:
	@rm -rf test/tmp test/q_test *.o

.PHONY: pull_core core_lib test clean
