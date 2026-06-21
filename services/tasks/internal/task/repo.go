package task

import (
	"errors"
	"time"
)

var ErrTaskNotFound = errors.New("task not found")

type Repo struct {
	data map[int64]Task
}

func NewRepo() *Repo {
	return &Repo{
		data: map[int64]Task{
			1: {
				ID:          1,
				Title:       "Изучить Redis",
				Description: "Разобрать cache-aside",
				DueDate:     time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC),
			},
			2: {
				ID:          2,
				Title:       "Сделать ПЗ",
				Description: "Реализовать кэширование по id",
				DueDate:     time.Date(2026, 1, 21, 0, 0, 0, 0, time.UTC),
			},
		},
	}
}

func (r *Repo) GetByID(id int64) (Task, error) {
	t, ok := r.data[id]
	if !ok {
		return Task{}, ErrTaskNotFound
	}
	return t, nil
}

func (r *Repo) Update(task Task) error {
	if _, ok := r.data[task.ID]; !ok {
		return ErrTaskNotFound
	}
	r.data[task.ID] = task
	return nil
}

func (r *Repo) Delete(id int64) error {
	if _, ok := r.data[id]; !ok {
		return ErrTaskNotFound
	}
	delete(r.data, id)
	return nil
}

// GetAll возвращает все задачи в виде среза
func (r *Repo) GetAll() []Task {
	tasks := make([]Task, 0, len(r.data))
	for _, t := range r.data {
		tasks = append(tasks, t)
	}
	return tasks
}

// Create добавляет новую задачу и возвращает её с присвоенным ID
func (r *Repo) Create(task Task) (Task, error) {
    var maxID int64
    for id := range r.data {
        if id > maxID {
            maxID = id
        }
    }
    task.ID = maxID + 1
    r.data[task.ID] = task
    return task, nil
}