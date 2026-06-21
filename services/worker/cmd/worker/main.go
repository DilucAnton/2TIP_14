package main

import (
    "encoding/json"
    "fmt"
    "log"
    "os"
    "time"
    "context"

    amqp "github.com/rabbitmq/amqp091-go"
    "example.com/worker/internal/jobs"
    "example.com/worker/internal/store"
)

const maxAttempts = 3

func main() {
    rabbitURL := os.Getenv("RABBIT_URL")
    if rabbitURL == "" {
        rabbitURL = "amqp://guest:guest@localhost:5672/"
    }
    queueName := "task_jobs"
    dlqName := "task_jobs_dlq"

    conn, err := amqp.Dial(rabbitURL)
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Failed to open channel: %v", err)
    }
    defer ch.Close()

    // Объявляем очереди (создаём, если нет)
    _, err = ch.QueueDeclare(dlqName, true, false, false, false, nil)
    if err != nil {
        log.Fatalf("Failed to declare DLQ: %v", err)
    }
    args := amqp.Table{
        "x-dead-letter-exchange":    "",
        "x-dead-letter-routing-key": dlqName,
    }
    _, err = ch.QueueDeclare(queueName, true, false, false, false, args)
    if err != nil {
        log.Fatalf("Failed to declare queue: %v", err)
    }

    // Prefetch = 1
    err = ch.Qos(1, 0, false)
    if err != nil {
        log.Fatalf("QoS error: %v", err)
    }

    msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
    if err != nil {
        log.Fatalf("Consume error: %v", err)
    }

    processedStore := store.NewProcessedStore()

    log.Println("Worker started, waiting for jobs...")
    for d := range msgs {
        var job jobs.TaskJob
        if err := json.Unmarshal(d.Body, &job); err != nil {
            log.Printf("Invalid message: %v", err)
            d.Nack(false, false)
            continue
        }

        // Идемпотентность
        if processedStore.Exists(job.MessageID) {
            log.Printf("Duplicate message %s, acking", job.MessageID)
            d.Ack(false)
            continue
        }

        // Имитируем обработку
        err := processJob(job)
        if err == nil {
            // Успех
            processedStore.MarkDone(job.MessageID)
            d.Ack(false)
            log.Printf("Job %s completed successfully", job.MessageID)
            continue
        }

        // Ошибка
        log.Printf("Job %s failed: %v, attempt %d/%d", job.MessageID, err, job.Attempt, maxAttempts)

        if job.Attempt < maxAttempts {
            // Retry: увеличиваем попытку и публикуем заново в ту же очередь
            job.Attempt++
            body, _ := json.Marshal(job)
            errPub := ch.PublishWithContext(context.Background(), "", queueName, false, false,
                amqp.Publishing{
                    ContentType:  "application/json",
                    DeliveryMode: amqp.Persistent,
                    Body:         body,
                })
            if errPub != nil {
                log.Printf("Failed to publish retry: %v", errPub)
                d.Nack(false, true) // вернуть в очередь
                continue
            }
            d.Ack(false)
            log.Printf("Job %s retry scheduled (attempt %d)", job.MessageID, job.Attempt)
        } else {
            // Превышено число попыток -> отправляем в DLQ
            body, _ := json.Marshal(job)
            errPub := ch.PublishWithContext(context.Background(), "", dlqName, false, false,
                amqp.Publishing{
                    ContentType:  "application/json",
                    DeliveryMode: amqp.Persistent,
                    Body:         body,
                })
            if errPub != nil {
                log.Printf("Failed to publish to DLQ: %v", errPub)
                d.Nack(false, true)
                continue
            }
            d.Ack(false)
            log.Printf("Job %s moved to DLQ after %d attempts", job.MessageID, maxAttempts)
        }
    }
}

func processJob(job jobs.TaskJob) error {
    // Имитация долгой обработки
    time.Sleep(2 * time.Second)

    // Управляемая ошибка: если task_id == "t_fail" или "fail" – ошибка
    if job.TaskID == "t_fail" || job.TaskID == "fail" {
        return fmt.Errorf("simulated processing error for task %s", job.TaskID)
    }
    // Иначе успех
    log.Printf("Processing task %s", job.TaskID)
    return nil
}