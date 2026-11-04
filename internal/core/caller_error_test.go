package core

import (
	"encoding/json"
	"net/http/httptest"
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
