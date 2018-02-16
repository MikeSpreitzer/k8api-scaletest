
# The following will put the built image in your Dockerhub namespace,
# and assumes you have one and it equals your username.  In general,
# set DOCKERPFX to identify a Docker $registry/$namespace.

DOCKERPFX=$(LOGNAME)

WATCH=dockerbuild/flunder-watch

.PHONY: build
build: ${WATCH}

${WATCH}: main.go hostaddr.go
	mkdir -p dockerbuild
	GOOS=linux GOARCH=amd64 go build -o ${WATCH}

publish: ${WATCH} Dockerfile
	cp Dockerfile dockerbuild/
	cd dockerbuild && docker build -t ${DOCKERPFX}/flunder-watch . && docker push ${DOCKERPFX}/flunder-watch
