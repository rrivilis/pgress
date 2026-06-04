# Makefile — SSE2 ternary propkernel  (Linux / WSL, x86-64)
#
# Requires: nasm >= 2.14, gcc (any recent version with -msse2 support)
# Usage:
#   make          — build and run witness tests
#   make build    — build only
#   make test     — run tests
#   make clean    — remove build artefacts

CC        ?= gcc

CFLAGS    := -O2 -msse2 -std=c11 -Wall -Wextra -g
ASFLAGS   := -msse2

.PHONY: all build test clean

all: test

build: test_propkernel

test_propkernel: test_propkernel.o ternary_propkernel.o
	$(CC) -o $@ $^

# GAS (.S) assembled by gcc — no nasm required
ternary_propkernel.o: ternary_propkernel.S
	$(CC) $(ASFLAGS) -c -o $@ $<

test_propkernel.o: test_propkernel.c
	$(CC) $(CFLAGS) -c -o $@ $<

test: build
	./test_propkernel

clean:
	rm -f *.o test_propkernel
