package db

import (
	"io/ioutil"
)

// Get a temporary db file.
func GetTempDB() (string, error) {
	tmpfile, err := ioutil.TempFile(".", "db-*")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()
	return tmpfile.Name(), nil
}
