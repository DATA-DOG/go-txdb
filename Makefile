test: MYSQL_DSN=root:pass@/txdb_test
test: PSQL_DSN=postgres://postgres:pass@localhost/txdb_test
test: mysql psql
	@go test -race
