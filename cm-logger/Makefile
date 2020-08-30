
# The following will put the built image in your Dockerhub namespace,
# and assumes you have one and it equals your username.  In general,
# set DOCKERPFX to identify a Docker $registry/$namespace.

DOCKERPFX=$(LOGNAME)
gitcommit=$(shell git rev-parse "HEAD^{commit}")$(shell if git_status=$$(git status --porcelain 2>/dev/null) && [ -z "$${git_status}" ]; then true ; else echo "-dirty"; fi)

LOGGER=dockerbuild/cm-logger

.PHONY: build
build: ${LOGGER}

${LOGGER}: main.go hostaddr.go
	mkdir -p dockerbuild
	GOOS=linux GOARCH=amd64 go build -o ${LOGGER}

publish: ${LOGGER} Dockerfile
	cp Dockerfile dockerbuild/
	cd dockerbuild && docker build -t ${DOCKERPFX}/cm-logger -t ${DOCKERPFX}/cm-logger:${gitcommit} . && docker push ${DOCKERPFX}/cm-logger && docker push ${DOCKERPFX}/cm-logger:${gitcommit}
