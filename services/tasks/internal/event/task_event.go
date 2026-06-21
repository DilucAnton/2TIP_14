package event

type TaskEvent struct {
    Event  string `json:"event"`
    TaskID int64  `json:"task_id"`
    TS     string `json:"ts"`
}