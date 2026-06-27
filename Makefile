# Build targets for rayforce-q
#
#   make test       # build the rayfall test driver and run the .rfl suite
#   make recheck    # re-run the suite against an already-built core
#   make rayforce   # build a rayforce binary with the Q IPC client+server in
#   make lint       # clang-format the sources
#
#   RAYFORCE_LOCAL_PATH=/path/to/core make test   # use a local core checkout
#
# Variables:
#   RAYFORCE_GITHUB      core git URL (default: public repo)
#   RAYFORCE_REF         branch/tag to check out (optional)
#   RAYFORCE_LOCAL_PATH  rsync this checkout instead of cloning

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

# The test driver links the reusable client (q.c), the reusable server
# (q_server.c), and the shipped `.q.*` verb glue (embed/rayforce_q.c)
DRIVER_SRCS = q.c q_server.c embed/rayforce_q.c test/driver.c

FMT_SRCS = q.c q.h q_server.c q_server.h embed/rayforce_q.c test/driver.c

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

test: pull_core
	@echo "🔨 Building librayforce.a..."
	@$(MAKE) -C $(CORE) lib
	@echo "🔧 Building test/driver (client + --serve)..."
	@$(CC) $(CFLAGS) -o test/driver $(DRIVER_SRCS) $(CORE)/librayforce.a $(LIBS)
	@./test/run.sh ./test/driver test/rfl

# Re-run the suite against an already-built core (fast; no clone/rebuild).
# Requires a prior `make test`. Used by the pre-commit hook.
recheck:
	@test -f $(CORE)/librayforce.a || { echo "no core build — run 'make test' first" >&2; exit 1; }
	@test ! -d $(CORE)/src/q || { echo "core was used by 'make rayforce' — run 'make clean && make test'" >&2; exit 1; }
	@$(CC) $(CFLAGS) -o test/driver $(DRIVER_SRCS) $(CORE)/librayforce.a $(LIBS)
	@./test/run.sh ./test/driver test/rfl

# Build a rayforce binary with the Q IPC client AND the Q-protocol server compiled in.
rayforce: pull_core
	@echo "🔧 Embedding Q IPC into the rayforce binary..."
	@mkdir -p $(CORE)/src/q
	@cp q.c q.h q_server.c q_server.h embed/rayforce_q.c $(CORE)/src/q/
	@sed -e 's@ray_runtime_t\* rt = ray_runtime_create(argc, argv);@& extern void q_env_register(void); if (rt) q_env_register();@' \
	     -e 's@        else if (strcmp(argv\[i\], "--") == 0)@        else if ((strcmp(argv[i], "-q") == 0 || strcmp(argv[i], "--q-serve") == 0) \&\& i + 1 < argc) { i++; } &@' \
	     -e 's@if (poll) ray_runtime_set_poll(poll);@& { extern int64_t q_serve_from_args(ray_poll_t*, int, char**); if (poll) q_serve_from_args(poll, argc, argv); }@' \
		$(CORE)/src/app/main.c > $(CORE)/src/app/main.c.q && \
		mv $(CORE)/src/app/main.c.q $(CORE)/src/app/main.c
	@$(MAKE) -C $(CORE) release
	@cp $(CORE)/rayforce ./rayforce
	@if [ "$(UNAME_S)" = "Darwin" ]; then codesign --force --sign - ./rayforce >/dev/null 2>&1 || true; fi
	@echo './rayforce is ready'

lint:
	clang-format -i $(addprefix ./,$(FMT_SRCS))

clean:
	@rm -rf test/tmp test/driver rayforce *.o

.PHONY: pull_core core_lib driver test recheck rayforce lint hooks clean
