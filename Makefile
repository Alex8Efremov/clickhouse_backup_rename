SHELL=/bin/bash
APP=main

.DEFAULT_GOAL := up

init: down
	@go mod init mod && go mod tidy

build:
	@go build -o ${APP} *.go 

up: build
	@./${APP} -d MyOldDB:myNewDB -t old_events:my_new_events backName
down:
	@rm ${APP} go.mod

.PHONY: all

all: down init build up
