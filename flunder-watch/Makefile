
# The following will put the built image in your Dockerhub namespace,
# and assumes you have one and it equals your username.  In general,
# set DOCKERPFX to identify a Docker $registry/$namespace.

DOCKERPFX=$(LOGNAME)
gitcommit=$(shell git rev-parse "HEAD^{commit}")$(shell if git_status=$$(git status --porcelain 2>/dev/null) && [ -z "$${git_status}" ]; then true ; else echo "-dirty"; fi)

WATCH=dockerbuild/flunder-watch

.PHONY: build
build: ${WATCH}

${WATCH}: main.go hostaddr.go
	mkdir -p dockerbuild
	GOOS=linux GOARCH=amd64 go build -o ${WATCH}

publish: ${WATCH} Dockerfile
	cp Dockerfile dockerbuild/
	cd dockerbuild && docker build -t ${DOCKERPFX}/flunder-watch -t ${DOCKERPFX}/flunder-watch:${gitcommit} . && docker push ${DOCKERPFX}/flunder-watch && docker push ${DOCKERPFX}/flunder-watch:${gitcommit}
