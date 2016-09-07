GO=GO15VENDOREXPERIMENT="1" go

.PHONY: build importer syncer test check deps

build: importer syncer check test

importer:
	$(GO) build -o bin/importer ./importer

syncer:
	$(GO) build -o bin/syncer ./syncer

test:

check:
	$(GO) get github.com/golang/lint/golint

	$(GO) tool vet . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	$(GO) tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	
update_vendor:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
ifdef PKG
	glide get --strip-vendor --skip-test ${PKG}
else
	glide update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests

