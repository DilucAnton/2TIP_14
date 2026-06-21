package service

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"example.com/pz9-redis-cache/internal/cache"
	"example.com/pz9-redis-cache/internal/config"
	"example.com/pz9-redis-cache/internal/task"
	"example.com/pz9-redis-cache/internal/event"
	"github.com/redis/go-redis/v9"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TaskService struct {
	repo  *task.Repo
	redis *redis.Client
	cfg   config.Config

	// Новые поля для RabbitMQ
    amqpChan   *amqp.Channel
    queueName  string
}

func NewTaskService(repo *task.Repo, redisClient *redis.Client, cfg config.Config, amqpChan *amqp.Channel, queueName string) *TaskService {
    return &TaskService{
        repo:       repo,
        redis:      redisClient,
        cfg:        cfg,
        amqpChan:   amqpChan,
        queueName:  queueName,
    }
}

func (s *TaskService) GetTaskByID(ctx context.Context, id int64) (task.Task, error) {
	key := cache.TaskByIDKey(id)

	if s.redis != nil {
		cached, err := s.redis.Get(ctx, key).Result()
		if err == nil {
			var t task.Task
			if err := json.Unmarshal([]byte(cached), &t); err == nil {
				log.Println("cache hit:", key)
				return t, nil
			}
			log.Println("cache decode error:", err)
		} else if !errors.Is(err, redis.Nil) {
			log.Println("redis read error:", err)
		} else {
			log.Println("cache miss:", key)
		}
	}

	t, err := s.repo.GetByID(id)
	if err != nil {
		return task.Task{}, err
	}

	if s.redis != nil {
		bytes, err := json.Marshal(t)
		if err != nil {
			log.Println("cache encode error:", err)
			return t, nil
		}

		ttl := cache.TTLWithJitter(s.cfg.CacheTTL, s.cfg.CacheTTLJitter)
		if err := s.redis.Set(ctx, key, bytes, ttl).Err(); err != nil {
			log.Println("redis write error:", err)
		}
	}

	return t, nil
}

func (s *TaskService) UpdateTask(ctx context.Context, t task.Task) error {
	if err := s.repo.Update(t); err != nil {
		return err
	}
	if s.redis != nil {
		key := cache.TaskByIDKey(t.ID)
		if err := s.redis.Del(ctx, key).Err(); err != nil {
			log.Println("redis delete error:", err)
		}
		// Инвалидация списка
		listKey := cache.TasksListKey()
		if err := s.redis.Del(ctx, listKey).Err(); err != nil {
			log.Println("redis delete list error:", err)
		}
	}
	return nil
}

func (s *TaskService) DeleteTask(ctx context.Context, id int64) error {
	if err := s.repo.Delete(id); err != nil {
		return err
	}

	if s.redis != nil {
		key := cache.TaskByIDKey(id)
		if err := s.redis.Del(ctx, key).Err(); err != nil {
			log.Println("redis delete error:", err)
		}
		// Инвалидация списка
		listKey := cache.TasksListKey()
		if err := s.redis.Del(ctx, listKey).Err(); err != nil {
			log.Println("redis delete list error:", err)
		}
	}
	return nil
}

// GetTasksList возвращает список всех задач с кэшированием
func (s *TaskService) GetTasksList(ctx context.Context) ([]task.Task, error) {
	key := cache.TasksListKey()

	// Пытаемся получить из кэша
	if s.redis != nil {
		cached, err := s.redis.Get(ctx, key).Result()
		if err == nil {
			var tasks []task.Task
			if err := json.Unmarshal([]byte(cached), &tasks); err == nil {
				log.Println("cache hit:", key)
				return tasks, nil
			}
			log.Println("cache list decode error:", err)
		} else if !errors.Is(err, redis.Nil) {
			log.Println("redis read error (list):", err)
		} else {
			log.Println("cache miss:", key)
		}
	}

	// Получаем все задачи из репозитория
	allTasks := s.repo.GetAll()  // нужно добавить метод GetAll в репозиторий
	if allTasks == nil {
		allTasks = []task.Task{}
	}

	// Записываем в кэш, если Redis доступен
	if s.redis != nil {
		bytes, err := json.Marshal(allTasks)
		if err != nil {
			log.Println("cache list encode error:", err)
			return allTasks, nil
		}
		ttl := cache.TTLWithJitter(s.cfg.CacheTTL, s.cfg.CacheTTLJitter)
		if err := s.redis.Set(ctx, key, bytes, ttl).Err(); err != nil {
			log.Println("redis write error (list):", err)
		}
	}
	return allTasks, nil
}

func (s *TaskService) publishTaskCreated(ctx context.Context, taskID int64) error {
    if s.amqpChan == nil {
        return nil // RabbitMQ не подключён, просто игнорируем
    }

    // Объявляем очередь (durable)
    _, err := s.amqpChan.QueueDeclare(
        s.queueName,
        true,  // durable
        false, // autoDelete
        false, // exclusive
        false, // noWait
        nil,
    )
    if err != nil {
        return err
    }

    // Формируем событие
    ev := event.TaskEvent{
        Event:  "task.created",
        TaskID: taskID,
        TS:     time.Now().UTC().Format(time.RFC3339),
    }
    body, err := json.Marshal(ev)
    if err != nil {
        return err
    }

    // Публикуем сообщение
    return s.amqpChan.PublishWithContext(
        ctx,
        "",          // exchange
        s.queueName, // routing key = имя очереди
        false,       // mandatory
        false,       // immediate
        amqp.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp.Persistent, // persistent message
            Body:         body,
        },
    )
}

// CreateTask создаёт новую задачу
func (s *TaskService) CreateTask(ctx context.Context, t task.Task) (task.Task, error) {
    created, err := s.repo.Create(t)
    if err != nil {
        return task.Task{}, err
    }
    // Публикация события
    if s.amqpChan != nil {
        if err := s.publishTaskCreated(ctx, created.ID); err != nil {
            log.Printf("Failed to publish task.created: %v", err)
        }
    }
    return created, nil
}