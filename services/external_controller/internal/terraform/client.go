package terraform

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
	baseURL      string
	token        string
	organization string
	httpClient   *http.Client
}

func NewClient(baseURL, token, organization string) *Client {
	if baseURL == "" {
		baseURL = "https://app.terraform.io"
	}
	return &Client{
		baseURL:      baseURL,
		token:        token,
		organization: organization,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

type Run struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Message     string `json:"message"`
	CreatedAt   string `json:"created-at"`
	HasChanges  bool   `json:"has-changes"`
	IsDestroy   bool   `json:"is-destroy"`
	WorkspaceID string `json:"-"`
}

type RunResponse struct {
	Data struct {
		ID         string `json:"id"`
		Attributes Run    `json:"attributes"`
	} `json:"data"`
}

func (c *Client) CreateRun(ctx context.Context, workspaceName, message string, isDestroy bool, vars map[string]string) (*Run, error) {
	workspaceID, err := c.getWorkspaceID(ctx, workspaceName)
	if err != nil {
		return nil, fmt.Errorf("get workspace: %w", err)
	}

	endpoint := fmt.Sprintf("%s/api/v2/runs", c.baseURL)

	payload := map[string]interface{}{
		"data": map[string]interface{}{
			"type": "runs",
			"attributes": map[string]interface{}{
				"message":    message,
				"is-destroy": isDestroy,
			},
			"relationships": map[string]interface{}{
				"workspace": map[string]interface{}{
					"data": map[string]interface{}{
						"type": "workspaces",
						"id":   workspaceID,
					},
				},
			},
		},
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

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("terraform returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var runResp RunResponse
	if err := json.NewDecoder(resp.Body).Decode(&runResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	run := &runResp.Data.Attributes
	run.ID = runResp.Data.ID
	run.WorkspaceID = workspaceID

	return run, nil
}

func (c *Client) GetRun(ctx context.Context, runID string) (*Run, error) {
	endpoint := fmt.Sprintf("%s/api/v2/runs/%s", c.baseURL, runID)

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
		return nil, fmt.Errorf("terraform returned status %d", resp.StatusCode)
	}

	var runResp RunResponse
	if err := json.NewDecoder(resp.Body).Decode(&runResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	run := &runResp.Data.Attributes
	run.ID = runResp.Data.ID

	return run, nil
}

func (c *Client) CancelRun(ctx context.Context, runID string) error {
	endpoint := fmt.Sprintf("%s/api/v2/runs/%s/actions/cancel", c.baseURL, runID)

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

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("terraform returned status %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) GetRunLogs(ctx context.Context, runID string) (string, error) {
	run, err := c.GetRun(ctx, runID)
	if err != nil {
		return "", err
	}

	endpoint := fmt.Sprintf("%s/api/v2/runs/%s/plan/log", c.baseURL, run.ID)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}

	return string(body), nil
}

func (c *Client) getWorkspaceID(ctx context.Context, name string) (string, error) {
	endpoint := fmt.Sprintf("%s/api/v2/organizations/%s/workspaces/%s", c.baseURL, c.organization, name)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("terraform returned status %d", resp.StatusCode)
	}

	var wsResp struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wsResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}

	return wsResp.Data.ID, nil
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/vnd.api+json")
}

func StatusToJobStatus(tfStatus string) string {
	switch tfStatus {
	case "pending", "queued", "planning", "plan_queued":
		return "running"
	case "planned", "cost_estimating", "cost_estimated", "policy_checking", "policy_checked", "confirmed":
		return "running"
	case "applying", "apply_queued":
		return "running"
	case "applied", "planned_and_finished":
		return "success"
	case "errored", "force_canceled":
		return "failed"
	case "canceled", "discarded":
		return "cancelled"
	default:
		return "pending"
	}
}
