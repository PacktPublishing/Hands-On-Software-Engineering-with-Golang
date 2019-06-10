package db_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq" // register postgres driver with the sql package.
)

func TestDBConnection(t *testing.T) {
	host, port, dbName, user, pass := os.Getenv("DB_HOST"), os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"), os.Getenv("DB_USER"), os.Getenv("DB_PASS")
	if host == "" {
		t.Skip("Skipping test as DB connection info is not present")
	}

	db, err := sql.Open("postgres", makeDSN(user, pass, dbName, host, port))
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	t.Log("Connection to DB succeeded")
}

// makeDSN creates a data source name for connecting to a postgres instance.
func makeDSN(user, pass, dbName, host, port string) string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbName,
	)
}
