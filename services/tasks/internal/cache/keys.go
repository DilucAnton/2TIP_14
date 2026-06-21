package cache

import "fmt"

func TaskByIDKey(id int64) string {
	return fmt.Sprintf("tasks:task:%d", id)
}

func TasksListKey() string {
	return "tasks:list"
}
