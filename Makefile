define MYSQL_SQL
CREATE TABLE users (
  id BIGINT UNSIGNED AUTO_INCREMENT NOT NULL,
  username VARCHAR(32) NOT NULL,
  email VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE INDEX uniq_email (email)
) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
endef

define PSQL_SQL
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(32) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL
);
endef

export MYSQL_SQL
MYSQL := "$$MYSQL_SQL"

export PSQL_SQL
PSQL := "$$PSQL_SQL"

INSERTS := "INSERT INTO users (username, email) VALUES ('gopher', 'gopher@go.com'), ('john', 'john@doe.com'), ('jane', 'jane@doe.com');"

MYSQLCMD=mysql
ifndef CI
	MYSQLCMD=docker-compose exec mysql mysql
endif

PSQLCMD=psql
ifndef CI
	PSQLCMD=docker-compose exec postgres psql
endif

test: mysql psql
	@go test -race -tags "mysql psql"

mysql:
	@$(MYSQLCMD) -h localhost -u root -e 'DROP DATABASE IF EXISTS txdb_test'
	@$(MYSQLCMD) -h localhost -u root -e 'CREATE DATABASE txdb_test'
	@$(MYSQLCMD) -h localhost -u root txdb_test -e $(MYSQL)
	@$(MYSQLCMD) -h localhost -u root txdb_test -e $(INSERTS)

psql:
	@$(PSQLCMD) -U postgres -c 'DROP DATABASE IF EXISTS txdb_test'
	@$(PSQLCMD) -U postgres -c 'CREATE DATABASE txdb_test'
	@$(PSQLCMD) -U postgres txdb_test -c $(PSQL)
	@$(PSQLCMD) -U postgres txdb_test -c $(INSERTS)

.PHONY: test mysql psql
