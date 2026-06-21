package main

import (
	"context"
	"log"
	"net/http"
	"encoding/json"
	"github.com/google/uuid"

	"example.com/pz9-redis-cache/internal/cache"
	"example.com/pz9-redis-cache/internal/config"
	"example.com/pz9-redis-cache/internal/httpapi"
	"example.com/pz9-redis-cache/internal/service"
	"example.com/pz9-redis-cache/internal/task"
	"example.com/pz9-redis-cache/internal/jobs"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	cfg := config.New()

	repo := task.NewRepo()
	redisClient := cache.NewRedisClient(cfg)

	if err := cache.Ping(context.Background(), redisClient); err != nil {
		log.Println("warning: redis is unavailable at startup:", err)
	}

	// ---------- RabbitMQ ----------
	var amqpChan *amqp.Channel
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Println("WARNING: cannot connect to RabbitMQ:", err)
	} else {
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			log.Println("WARNING: cannot open RabbitMQ channel:", err)
		} else {
			amqpChan = ch
		}
	}
	queueNameJobs := "task_jobs"
	dlqName := "task_jobs_dlq"

	if amqpChan != nil {
		// Объявляем DLQ
		_, err = amqpChan.QueueDeclare(dlqName, true, false, false, false, nil)
		if err != nil {
			log.Fatalf("Failed to declare DLQ: %v", err)
		}
		// Объявляем основную очередь с dead-letter настройкой
		args := amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": dlqName,
		}
		_, err = amqpChan.QueueDeclare(queueNameJobs, true, false, false, false, args)
		if err != nil {
			log.Fatalf("Failed to declare task_jobs: %v", err)
		}
		log.Println("RabbitMQ queues declared: task_jobs, task_jobs_dlq")
	}

	// ---------- Инициализация сервисов ----------
	taskService := service.NewTaskService(repo, redisClient, cfg, amqpChan, "task_events") // queueName для событий (не используется в jobs)
	handler := httpapi.NewHandler(taskService)

	// ---------- HTTP маршруты ----------
	mux := http.NewServeMux()

	// Старые маршруты для работы с задачами (REST API)
	mux.HandleFunc("/v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handler.GetTasksList(w, r)
		case http.MethodPost:
			handler.CreateTask(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/v1/tasks/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/tasks/" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			handler.GetTaskByID(w, r)
		case http.MethodPatch:
			handler.PatchTask(w, r)
		case http.MethodDelete:
			handler.DeleteTask(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// НОВЫЙ МАРШРУТ: постановка задачи в очередь (producer)
	mux.HandleFunc("POST /v1/jobs/process-task", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TaskID string `json:"task_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if req.TaskID == "" {
			http.Error(w, "task_id required", http.StatusBadRequest)
			return
		}

		if amqpChan == nil {
			http.Error(w, "RabbitMQ not available", http.StatusServiceUnavailable)
			return
		}

		// Формируем сообщение задачи
		job := jobs.TaskJob{
			Job:       "process_task",
			TaskID:    req.TaskID,
			Attempt:   1,
			MessageID: uuid.New().String(),
		}
		body, err := json.Marshal(job)
		if err != nil {
			log.Printf("JSON marshal error: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		// Публикуем в очередь task_jobs
		err = amqpChan.PublishWithContext(r.Context(), "", queueNameJobs, false, false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Body:         body,
			})
		if err != nil {
			log.Printf("Publish error: %v", err)
			http.Error(w, "failed to publish", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "accepted",
			"task_id": req.TaskID,
		})
	})

	log.Println("server started on :8082")
	if err := http.ListenAndServe(":8082", mux); err != nil {
		log.Fatal(err)
	}
}