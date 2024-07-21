package list

import (
	"errors"
	"io"
	"strings"

	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	return &List{nil, nil}
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	if list.head == nil {
		list.head = &Link{list, nil, nil, value}
		list.tail = list.head
	} else {
		list.head = &Link{list, nil, list.head, value}
		list.head.next.prev = list.head
	}
	return list.head
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	if list.tail == nil {
		list.tail = &Link{list, nil, nil, value}
		list.head = list.tail
	} else {
		list.tail = &Link{list, list.tail, nil, value}
		list.tail.prev.next = list.tail
	}
	return list.tail
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	for link := list.head; link != nil; link = link.next {
		if f(link) {
			return link
		}
	}
	return nil
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	for link := list.head; link != nil; link = link.next {
		f(link)
	}
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	return link.list
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	return link.value
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	link.value = value
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	return link.prev
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	return link.next
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	if link.prev == nil {
		link.list.head = link.next
	} else {
		link.prev.next = link.next
	}
	if link.next == nil {
		link.list.tail = link.prev
	} else {
		link.next.prev = link.prev
	}
	link.list = nil
	link.prev = nil
	link.next = nil
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	curRepl := repl.NewRepl()
	curRepl.AddCommand("list_print",
		func(args string, config *repl.REPLConfig) error {
			if list.head == nil {
				io.WriteString(config.GetWriter(), "List: "+"\n")
				return nil
			}
			vals := make([]string, 0)
			for link := list.head; link != nil; link = link.next {
				vals = append(vals, link.value.(string))
			}
			output := strings.Join(vals, ", ")
			io.WriteString(config.GetWriter(), output+"\n")
			return nil
		},
		"Print the list.")
	curRepl.AddCommand("list_push_head",
		func(args string, config *repl.REPLConfig) error {
			parts := strings.Split(args, " ")
			if len(parts) != 2 {
				return errors.New("invalid number of arguments")
			}
			list.PushHead(parts[1])
			return nil
		},
		"Push a value to the head of the list.")
	curRepl.AddCommand("list_push_tail",
		func(args string, config *repl.REPLConfig) error {
			parts := strings.Split(args, " ")
			if len(parts) != 2 {
				return errors.New("invalid number of arguments")
			}
			list.PushTail(parts[1])
			return nil
		},
		"Push a value to the tail of the list.")
	curRepl.AddCommand("list_remove",
		func(args string, config *repl.REPLConfig) error {
			parts := strings.Split(args, " ")
			if len(parts) != 2 {
				return errors.New("invalid number of arguments")
			}
			link := list.Find(func(link *Link) bool {
				return link.value == parts[1]
			})
			if link == nil {
				return errors.New("value not found")
			}
			link.PopSelf()
			return nil
		},
		"Remove a value from the list.")
	curRepl.AddCommand("list_contains",
		func(args string, config *repl.REPLConfig) error {
			parts := strings.Split(args, " ")
			if len(parts) != 2 {
				return errors.New("invalid number of arguments")
			}
			link := list.Find(func(link *Link) bool {
				return link.value == parts[1]
			})
			if link == nil {
				io.WriteString(config.GetWriter(), "not found\n")
			} else {
				io.WriteString(config.GetWriter(), "found\n")
			}
			return nil
		},
		"Check if a value is in the list.")
	return curRepl
}
