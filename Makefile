define DB_SQL
CREATE TABLE users (
  id BIGINT UNSIGNED AUTO_INCREMENT NOT NULL,
  username VARCHAR(32) NOT NULL,
  email VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE INDEX uniq_email (email)
) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;

INSERT INTO users(username, email) VALUES
  ("gopher", "gopher@go.com"),
  ("john", "john@doe.com"),
  ("jane", "jane@doe.com");
endef

export DB_SQL

SQL := "$$DB_SQL"

lint:
	@go fmt ./...
	@golint ./...
	@go vet ./...

test: db
	@go test

db:
	@mysql -u root -e 'DROP DATABASE IF EXISTS `txdb_test`'
	@mysql -u root -e 'CREATE DATABASE IF NOT EXISTS `txdb_test`'
	@mysql -u root txdb_test -e $(SQL)

cover: db
	go test -race -coverprofile=coverage.txt
	go tool cover -html=coverage.txt
	rm coverage.txt

.PHONY: test db lint cover
