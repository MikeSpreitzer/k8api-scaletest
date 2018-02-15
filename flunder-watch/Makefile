DOCKERREG=localhost:5000
WATCH=dockerbuild/flunder-watch

.PHONY: build
build: ${WATCH}

${WATCH}: main.go hostaddr.go
	mkdir -p dockerbuild
	GOOS=linux GOARCH=amd64 go build -o ${WATCH}

publish: ${WATCH} Dockerfile
	cp Dockerfile dockerbuild/
	cd dockerbuild && docker build -t ${DOCKERREG}/flunder-watch . && docker push ${DOCKERREG}/flunder-watch
