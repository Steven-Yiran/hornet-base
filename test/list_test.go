package test

import (
	"testing"

	list "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/list"
)

func TestBasicList(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushHead(2)
	l.PushHead(3)
	l.PushTail(4)
	l.PushTail(5)
	l.PushTail(6)

	if l.PeekHead().GetKey() != 3 {
		t.Fatal("bad head")
	}
	if l.PeekTail().GetKey() != 6 {
		t.Fatal("bad tail")
	}
}

func TestFind(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushHead(2)
	l.PushHead(3)
	l.PushTail(4)
	l.PushTail(5)
	l.PushTail(6)

	if l.Find(func(link *list.Link) bool { return link.GetKey() == 4 }) == nil {
		t.Fatal("bad find")
	}
	if l.Find(func(link *list.Link) bool { return link.GetKey() == 7 }) != nil {
		t.Fatal("bad find")
	}
}

func TestMap(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushHead(2)
	l.PushHead(3)
	l.PushTail(4)
	l.PushTail(5)
	l.PushTail(6)

	l.Map(func(link *list.Link) {
		link.SetKey(link.GetKey().(int) * 2)
	})

	if l.PeekHead().GetKey() != 6 {
		t.Fatal("bad map")
	}
	if l.PeekTail().GetKey() != 12 {
		t.Fatal("bad map")
	}
}
