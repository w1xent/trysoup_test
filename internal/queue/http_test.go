package queue_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"soupqueue/internal/httpapi"
	"soupqueue/internal/queue"
)

func TestHTTPContract(t *testing.T) {
	s, _ := database(t)
	server := httptest.NewServer(httpapi.Handler(s))
	defer server.Close()
	call := func(method, path, key, body string, want int) []byte {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		must(t, err)
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		res, err := server.Client().Do(req)
		must(t, err)
		defer res.Body.Close()
		raw, err := io.ReadAll(res.Body)
		must(t, err)
		if res.StatusCode != want {
			t.Fatalf("%s %s got %d %s, want %d", method, path, res.StatusCode, raw, want)
		}
		return raw
	}
	const body = `{"name":"test","duration_ms":3}`
	call("GET", "/health", "", "", 200)
	call("POST", "/jobs", "", body, 400)
	call("POST", "/jobs", "invalid", body+" {}", 400)
	call("POST", "/jobs", "invalid", `{"name":"a","duration_ms":0}`, 400)
	call("POST", "/jobs", "invalid", `{"name":"a","duration_ms":1,"extra":1}`, 400)
	call("GET", "/jobs?limit=0", "", "", 400)
	call("GET", "/jobs?after=-1", "", "", 400)
	call("GET", "/jobs/999", "", "", 404)
	call("POST", "/jobs/999/cancel", "", "", 404)
	first := call("POST", "/jobs", "one", body, 201)
	var j queue.Job
	must(t, json.Unmarshal(first, &j))
	if j.State != "queued" || j.Attempt != 0 {
		t.Fatal(j)
	}
	call("POST", "/jobs", "one", `{"should_fail":false,"duration_ms":3,"name":"test"}`, 200)
	call("POST", "/jobs", "one", `{"name":"different","duration_ms":3}`, 409)
	path := "/jobs/" + strconv.FormatInt(j.ID, 10)
	for range 2 {
		must(t, json.Unmarshal(call("POST", path+"/cancel", "", "", 200), &j))
		if j.State != "cancelled" || j.Attempt != 0 {
			t.Fatal(j)
		}
	}
	must(t, json.Unmarshal(call("POST", "/jobs", "one", body, 200), &j))
	if j.State != "cancelled" {
		t.Fatal(j)
	}
	call("POST", "/jobs", "two", body, 201)
	var page struct {
		Items []queue.Job
		Next  *int64 `json:"next_after"`
	}
	must(t, json.Unmarshal(call("GET", "/jobs?limit=1", "", "", 200), &page))
	if len(page.Items) != 1 || page.Items[0].ID != j.ID || page.Next == nil || *page.Next != j.ID {
		t.Fatal(page)
	}
	must(t, json.Unmarshal(call("GET", "/jobs?limit=1&after="+strconv.FormatInt(j.ID, 10), "", "", 200), &page))
	if len(page.Items) != 1 || page.Items[0].Key != "two" || page.Next != nil {
		t.Fatal(page)
	}
}
