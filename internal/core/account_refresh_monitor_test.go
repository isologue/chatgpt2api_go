package core

import "testing"

func TestAccountRefreshTrackerRecordsRoundsBySource(t *testing.T) {
	tracker := newAccountRefreshTracker()
	tracker.begin("hot_maintenance", "account_info", 0)
	tracker.beginItem("hot_maintenance", true, true)
	tracker.beginItem("hot_maintenance", true, true)
	tracker.finishItem("hot_maintenance", "a1", "a1@example.com", "")
	tracker.finishItem("hot_maintenance", "a2", "a2@example.com", "upstream timeout")
	tracker.tokenResult("hot_maintenance", nil)
	tracker.end("hot_maintenance", "completed")

	runs := tracker.snapshot()
	var hot accountRefreshRun
	for _, run := range runs {
		if run.Source == "hot_maintenance" {
			hot = run
		}
	}
	if hot.Status != "completed" || hot.Total != 2 || hot.Processed != 2 || hot.Succeeded != 1 || hot.Failed != 1 || hot.Active != 0 {
		t.Fatalf("unexpected hot maintenance result: %+v", hot)
	}
	if hot.HotAccounts != 2 || hot.ColdAccounts != 0 || hot.TokenRefreshOK != 1 || len(hot.Errors) != 1 {
		t.Fatalf("unexpected source or RT breakdown: %+v", hot)
	}
	if hot.Errors[0].Email != "a2@example.com" || hot.Errors[0].Message != "upstream timeout" {
		t.Fatalf("failure detail not retained: %+v", hot.Errors[0])
	}
}

func TestOnDemandRefreshRunClosesWhenLastCheckFinishes(t *testing.T) {
	tracker := newAccountRefreshTracker()
	tracker.beginOnDemandItem(false)
	if _, ended := tracker.finishItem("on_demand", "a1", "a1@example.com", ""); !ended {
		t.Fatal("expected the on-demand round to close")
	}
	runs := tracker.snapshot()
	if runs[0].Source != "on_demand" || runs[0].Status != "completed" || runs[0].Total != 1 || runs[0].Succeeded != 1 {
		t.Fatalf("unexpected on-demand result: %+v", runs[0])
	}
	if _, ended := tracker.end("missing", "completed"); ended {
		t.Fatal("ending an unknown round must not produce a result")
	}
}
