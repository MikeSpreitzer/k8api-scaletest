DOCKERREG=localhost:5000
LOGGER=dockerbuild/flunder-logger

.PHONY: build
build: ${LOGGER}

${LOGGER}: main.go hostaddr.go
	mkdir -p dockerbuild
	GOOS=linux GOARCH=amd64 go build -o ${LOGGER}

publish: ${LOGGER} Dockerfile
	cp Dockerfile dockerbuild/
	cd dockerbuild && docker build -t ${DOCKERREG}/flunder-logger . && docker push ${DOCKERREG}/flunder-logger
