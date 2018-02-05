DOCKERREG=localhost:5000
LOGGER=dockerbuild/obj-logger

.PHONY: build
build: ${LOGGER}

${LOGGER}: main.go hostaddr.go
	mkdir -p dockerbuild
	GOOS=linux GOARCH=amd64 go build -o ${LOGGER}

publish: ${LOGGER} Dockerfile
	cp Dockerfile dockerbuild/
	cd dockerbuild && docker build -t ${DOCKERREG}/object-logger . && docker push ${DOCKERREG}/object-logger
