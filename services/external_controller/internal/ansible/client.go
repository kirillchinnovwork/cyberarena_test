package ansible

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	baseURL    string
	apiToken   string
	httpClient *http.Client
}

func NewClient(baseURL, apiToken string) *Client {
	return &Client{
		baseURL:  baseURL,
		apiToken: apiToken,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

type Task struct {
	ID          int    `json:"id"`
	TemplateID  int    `json:"template_id"`
	ProjectID   int    `json:"project_id"`
	Status      string `json:"status"`
	Result      string `json:"result"`
	Message     string `json:"message"`
	Start       string `json:"start"`
	End         string `json:"end"`
	CreatedAt   string `json:"created"`
	UserID      int    `json:"user_id"`
	BuildTaskID int    `json:"build_task_id"`
}

type TaskOutput struct {
	TaskID int    `json:"task_id"`
	Task   string `json:"task"`
	Time   string `json:"time"`
	Output string `json:"output"`
}

func (c *Client) RunTask(ctx context.Context, projectID, templateID int, extraVars map[string]interface{}) (*Task, error) {
	endpoint := fmt.Sprintf("%s/api/project/%d/tasks", c.baseURL, projectID)

	payload := map[string]interface{}{
		"template_id": templateID,
		"project_id":  projectID,
	}

	if len(extraVars) > 0 {
		varsJSON, _ := json.Marshal(extraVars)
		payload["environment"] = string(varsJSON)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("semaphore returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var task Task
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &task, nil
}

func (c *Client) GetTask(ctx context.Context, projectID, taskID int) (*Task, error) {
	endpoint := fmt.Sprintf("%s/api/project/%d/tasks/%d", c.baseURL, projectID, taskID)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("semaphore returned status %d", resp.StatusCode)
	}

	var task Task
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &task, nil
}

func (c *Client) GetTaskOutput(ctx context.Context, projectID, taskID int) ([]TaskOutput, error) {
	endpoint := fmt.Sprintf("%s/api/project/%d/tasks/%d/output", c.baseURL, projectID, taskID)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("semaphore returned status %d", resp.StatusCode)
	}

	var outputs []TaskOutput
	if err := json.NewDecoder(resp.Body).Decode(&outputs); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return outputs, nil
}

func (c *Client) StopTask(ctx context.Context, projectID, taskID int) error {
	endpoint := fmt.Sprintf("%s/api/project/%d/tasks/%d/stop", c.baseURL, projectID, taskID)

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("semaphore returned status %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
}

func StatusToJobStatus(semStatus string) string {
	switch semStatus {
	case "waiting":
		return "pending"
	case "running":
		return "running"
	case "stopping":
		return "running"
	case "stopped":
		return "cancelled"
	case "success":
		return "success"
	case "error":
		return "failed"
	default:
		return "pending"
	}
}
