# Build targets for rayforce-q. The native rayforce engine is cloned (or
# copied from a local checkout) and built; both targets reuse that checkout.
#
#   make test       # build the rayfall test driver and run the .rfl suite
#   make recheck    # re-run the suite against an already-built core (fast)
#   make rayforce   # build a rayforce binary with Q IPC as .q.* env functions
#   make lint       # clang-format the sources
#   make hooks      # install the lint+test pre-commit hook
#
#   RAYFORCE_LOCAL_PATH=/path/to/core make test   # use a local core checkout
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

test: q_test
	@./test/run_tests.sh ./test/q_test test/rfl

# Re-run the suite against an already-built core (fast; no clone/rebuild).
# Requires a prior `make test`. Used by the pre-commit hook.
recheck:
	@test -f $(CORE)/librayforce.a || { echo "no core build — run 'make test' first" >&2; exit 1; }
	@test ! -d $(CORE)/src/q || { echo "core was used by 'make rayforce' — run 'make clean && make test'" >&2; exit 1; }
	@$(CC) $(CFLAGS) -o test/q_test $(SRCS) $(CORE)/librayforce.a $(LIBS)
	@./test/run_tests.sh ./test/q_test test/rfl

# Build a rayforce binary with the Q IPC client compiled in and exposed as the
# .q.connect / .q.send / .q.close rayfall env functions. Drops q.c/q.h/q_env.c
# into the core's src/ (auto-picked up by its wildcard build) and patches main.c
# to register them once, right after the runtime is created.
rayforce: pull_core
	@echo "🔧 Embedding Q IPC into the rayforce binary..."
	@mkdir -p $(CORE)/src/q
	@cp q.c q.h embed/q_env.c $(CORE)/src/q/
	@sed 's@ray_runtime_t\* rt = ray_runtime_create(argc, argv);@& extern void q_env_register(void); if (rt) q_env_register();@' \
		$(CORE)/src/app/main.c > $(CORE)/src/app/main.c.q && \
		mv $(CORE)/src/app/main.c.q $(CORE)/src/app/main.c
	@$(MAKE) -C $(CORE) release
	@cp $(CORE)/rayforce ./rayforce
	@echo '✓ ./rayforce ready — e.g. (.q.connect "localhost" 5000 "" "" 0)'

lint:
	clang-format -i ./q.c ./q.h ./embed/q_env.c ./test/q_builtins.c ./test/q_test.c

# Enable the pre-commit hook (clang-format check + fast tests).
hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit
	@echo "✓ pre-commit hook enabled (lint + tests)"

clean:
	@rm -rf test/tmp test/q_test rayforce *.o

.PHONY: pull_core core_lib test recheck rayforce lint hooks clean
