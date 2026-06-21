package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"example.com/pz9-redis-cache/internal/service"
	"example.com/pz9-redis-cache/internal/task"
)

type Handler struct {
	service *service.TaskService
}

func NewHandler(service *service.TaskService) *Handler {
	return &Handler{service: service}
}

func (h *Handler) GetTaskByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rawID := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	t, err := h.service.GetTaskByID(r.Context(), id)
	if err != nil {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(t)
}

func (h *Handler) PatchTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var t task.Task
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	if err := h.service.UpdateTask(r.Context(), t); err != nil {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) DeleteTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rawID := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	if err := h.service.DeleteTask(r.Context(), id); err != nil {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) GetTasksList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tasks, err := h.service.GetTasksList(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(tasks)
}

func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }
    var t task.Task
    if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
        http.Error(w, "bad json", http.StatusBadRequest)
        return
    }
    // Можно установить DueDate, если не задано
    if t.DueDate.IsZero() {
        t.DueDate = time.Now()
    }
    created, err := h.service.CreateTask(r.Context(), t)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(created)
}