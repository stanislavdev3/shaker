package main

import "testing"

func TestParseCommandWithProvider(t *testing.T) {
	command, err := parseCommand([]string{"--config", "/etc/shaker/provider.toml", "provider-worker", "kndc"})
	if err != nil {
		t.Fatal(err)
	}
	if command.configPath != "/etc/shaker/provider.toml" || command.role != "provider-worker" ||
		command.provider != "kndc" || len(command.args) != 0 {
		t.Fatalf("command=%+v", command)
	}
}

func TestParseCommandPreservesBackfillFlags(t *testing.T) {
	command, err := parseCommand([]string{"--config", "shaker.toml", "backfill", "--from", "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if command.role != "backfill" || len(command.args) != 2 || command.args[0] != "--from" {
		t.Fatalf("command=%+v", command)
	}
}

func TestParseCommandRequiresProvider(t *testing.T) {
	if _, err := parseCommand([]string{"provider-worker"}); err == nil {
		t.Fatal("expected missing provider error")
	}
}

func TestParseCommandRejectsConfigFlagAfterRole(t *testing.T) {
	if _, err := parseCommand([]string{"api", "--config", "shaker.toml"}); err == nil {
		t.Fatal("expected misplaced config flag error")
	}
}
