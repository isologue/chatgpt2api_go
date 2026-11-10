package core

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPollBudgetTimeoutUsesFriendlyCallerError(t *testing.T) {
	internal := uncertainImageResult("结果补查预算耗尽，未拿到图片标识")
	public := callerFacingError(internal)
	if public.Status != 400 || public.Code != "image_generation_failed" || public.Message != "未生成图片，请重试" {
		t.Fatalf("unexpected public error: %+v", public)
	}
	if internal.Status != 504 || internal.Code != "image_result_uncertain" || internal.Message != "结果补查预算耗尽，未拿到图片标识" {
		t.Fatalf("internal error was mutated: %+v", internal)
	}

	w := httptest.NewRecorder()
	sendError(w, public)
	if w.Code != 400 {
		t.Fatalf("HTTP status = %d, want 400", w.Code)
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "image_generation_failed" || body.Error.Message != "未生成图片，请重试" {
		t.Fatalf("unexpected response body: %s", w.Body.String())
	}
}

func TestCallerFacingErrorLeavesOtherUncertainErrorsAlone(t *testing.T) {
	internal := uncertainImageResult("上游生成流硬超时，未取得可补查的图片结果")
	if got := callerFacingError(internal); got != internal {
		t.Fatalf("unrelated uncertain error was remapped: %+v", got)
	}
}

func TestTotalTimeoutUsesFriendlyCallerError(t *testing.T) {
	internal := Err(504, "image_total_timeout", "图片请求总预算已耗尽")
	internal.rawResponse = []byte(`{"error":{"code":"upstream_timeout"}}`)
	public := callerFacingError(internal)
	if public.Status != 400 || public.Code != "image_generation_failed" || public.Message != "很抱歉，未能生成图片，请重试" {
		t.Fatalf("unexpected public error: %+v", public)
	}
	if internal.Status != 504 || internal.Code != "image_total_timeout" || internal.Message != "图片请求总预算已耗尽" {
		t.Fatalf("internal error was mutated: %+v", internal)
	}

	w := httptest.NewRecorder()
	sendError(w, public)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "很抱歉，未能生成图片，请重试") {
		t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
	}
}

func TestContentPolicyCallerMessageSyncAndStoredAsync(t *testing.T) {
	const want = "非常抱歉，该提示可能违反了我们的内容政策。如果你认为此判断有误，请重试或修改提示语。"
	internal := Err(400, "content_policy_violation", "上游安全审核拒绝")
	internal.rawResponse = []byte(`{"error":{"reason":"original upstream refusal"}}`)
	public := callerFacingError(internal)
	if public.Message != want || public.Status != 400 || public.Code != internal.Code || public.Retry || public.Penalty != 0 {
		t.Fatalf("unexpected policy response: %+v", public)
	}
	if internal.Message != "上游安全审核拒绝" || string(public.rawResponse) != string(internal.rawResponse) {
		t.Fatal("stored error or upstream evidence changed")
	}
	w := httptest.NewRecorder()
	sendError(w, public)
	if w.Code != 400 || !strings.Contains(w.Body.String(), want) {
		t.Fatal(w.Code, w.Body.String())
	}

	c := testConfig(t)
	store := newMemoryStore()
	// 模拟升级前已经失败的异步任务，不需要重新生成或改写历史记录。
	if err := store.CreateTask(context.Background(), Task{ID: "task_policy_old", Status: "failed", Error: internal}); err != nil {
		t.Fatal(err)
	}
	e := NewEngine(c, NewPool(nil, 1), store, nil)
	defer e.Close()
	w = requestJSON(t, NewHandler(e), "GET", "/v1/images/tasks/task_policy_old", "", c.APIKey)
	if w.Code != 200 || !strings.Contains(w.Body.String(), want) || !strings.Contains(w.Body.String(), `"status":"failed"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	task, err := store.GetTask(context.Background(), "task_policy_old")
	if err != nil || task.Error.Message != "上游安全审核拒绝" {
		t.Fatal("history rewritten", err)
	}
}
