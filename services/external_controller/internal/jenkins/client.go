package jenkins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	baseURL    string
	username   string
	apiToken   string
	httpClient *http.Client
}

func NewClient(baseURL, username, apiToken string) *Client {
	return &Client{
		baseURL:  baseURL,
		username: username,
		apiToken: apiToken,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type BuildInfo struct {
	Number    int    `json:"number"`
	URL       string `json:"url"`
	Building  bool   `json:"building"`
	Result    string `json:"result"`
	Timestamp int64  `json:"timestamp"`
	Duration  int64  `json:"duration"`
}

type QueueItem struct {
	ID         int        `json:"id"`
	Executable *BuildInfo `json:"executable"`
	Blocked    bool       `json:"blocked"`
	Buildable  bool       `json:"buildable"`
	Cancelled  bool       `json:"cancelled"`
}

func (c *Client) TriggerBuild(ctx context.Context, jobName string, params map[string]string) (int64, error) {
	endpoint := fmt.Sprintf("%s/job/%s/buildWithParameters", c.baseURL, url.PathEscape(jobName))

	formData := url.Values{}
	for k, v := range params {
		formData.Set(k, v)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBufferString(formData.Encode()))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	req.SetBasicAuth(c.username, c.apiToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("jenkins returned status %d: %s", resp.StatusCode, string(body))
	}

	location := resp.Header.Get("Location")
	if location == "" {
		return 0, fmt.Errorf("no queue location in response")
	}

	var queueID int64
	fmt.Sscanf(location, "%s/queue/item/%d/", new(string), &queueID)

	return queueID, nil
}

func (c *Client) GetQueueItem(ctx context.Context, queueID int64) (*QueueItem, error) {
	endpoint := fmt.Sprintf("%s/queue/item/%d/api/json", c.baseURL, queueID)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.SetBasicAuth(c.username, c.apiToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jenkins returned status %d", resp.StatusCode)
	}

	var item QueueItem
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &item, nil
}

func (c *Client) GetBuildInfo(ctx context.Context, jobName string, buildNumber int) (*BuildInfo, error) {
	endpoint := fmt.Sprintf("%s/job/%s/%d/api/json", c.baseURL, url.PathEscape(jobName), buildNumber)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.SetBasicAuth(c.username, c.apiToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jenkins returned status %d", resp.StatusCode)
	}

	var info BuildInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &info, nil
}

func (c *Client) GetBuildLog(ctx context.Context, jobName string, buildNumber int, start int64) (string, int64, bool, error) {
	endpoint := fmt.Sprintf("%s/job/%s/%d/logText/progressiveText?start=%d", c.baseURL, url.PathEscape(jobName), buildNumber, start)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return "", 0, false, fmt.Errorf("create request: %w", err)
	}
	req.SetBasicAuth(c.username, c.apiToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", 0, false, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, false, fmt.Errorf("read body: %w", err)
	}

	var newOffset int64
	fmt.Sscanf(resp.Header.Get("X-Text-Size"), "%d", &newOffset)

	moreData := resp.Header.Get("X-More-Data") == "true"

	return string(body), newOffset, moreData, nil
}

func (c *Client) StopBuild(ctx context.Context, jobName string, buildNumber int) error {
	endpoint := fmt.Sprintf("%s/job/%s/%d/stop", c.baseURL, url.PathEscape(jobName), buildNumber)

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.SetBasicAuth(c.username, c.apiToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusFound {
		return fmt.Errorf("jenkins returned status %d", resp.StatusCode)
	}

	return nil
}
