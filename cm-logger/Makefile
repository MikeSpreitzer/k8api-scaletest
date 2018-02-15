DOCKERREG=localhost:5000
LOGGER=dockerbuild/cm-logger

.PHONY: build
build: ${LOGGER}

${LOGGER}: main.go hostaddr.go
	mkdir -p dockerbuild
	GOOS=linux GOARCH=amd64 go build -o ${LOGGER}

publish: ${LOGGER} Dockerfile
	cp Dockerfile dockerbuild/
	cd dockerbuild && docker build -t ${DOCKERREG}/cm-logger . && docker push ${DOCKERREG}/cm-logger
