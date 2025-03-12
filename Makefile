BUNDLE_PATH := "tmp/bundle"

all: test build

build: 
	go generate ./...
	go build -o bin/icepq ./cmd

bundle: build 
	mkdir -p ${BUNDLE_PATH}
	mkdir -p ${BUNDLE_PATH}/etc/clickhouse-server
	mkdir -p ${BUNDLE_PATH}/var/lib/clickhouse/user_scripts
	cp bin/icepq ${BUNDLE_PATH}/var/lib/clickhouse/user_scripts/
	cp config/*_function.*ml ${BUNDLE_PATH}/etc/clickhouse-server/
	COPYFILE_DISABLE=1 tar --no-xattr -cvzf ${BUNDLE_PATH}/../bundle.tar.gz -C ${BUNDLE_PATH} .
	
test:
	go test -v ./...

clean:
	rm -rf bin
