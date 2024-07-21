package test

import (
	"testing"

	list "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/list"
)

func TestSample(t *testing.T) {
	l := list.NewList()
	if l.PeekHead() != nil || l.PeekTail() != nil {
		t.Fatal("bad list initialization")
	}
}
