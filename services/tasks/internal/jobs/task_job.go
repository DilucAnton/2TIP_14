package jobs

type TaskJob struct {
    Job       string `json:"job"`
    TaskID    string `json:"task_id"`
    Attempt   int    `json:"attempt"`
    MessageID string `json:"message_id"`
}