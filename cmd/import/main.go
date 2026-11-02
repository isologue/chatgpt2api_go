package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/isologue/chatgpt2api-go/internal/core"
	"io"
	"os"
	"time"
)

func main() {
	file := flag.String("file", "-", "account export JSON file, or - for stdin")
	flag.Parse()
	if err := run(*file); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(file string) error {
	var reader io.Reader = os.Stdin
	if file != "-" {
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("cannot open import file")
		}
		defer f.Close()
		reader = f
	}
	raw, err := io.ReadAll(io.LimitReader(reader, 256<<20+1))
	if err != nil || len(raw) > 256<<20 {
		return fmt.Errorf("import file exceeds limit")
	}
	accounts, err := core.ParseAccounts(raw)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s, err := core.OpenPostgres(ctx, os.Getenv("CORE_DATABASE_URL"), 4)
	if err != nil {
		return err
	}
	defer s.Close()
	if err = s.AcquireInstance(ctx); err != nil {
		return fmt.Errorf("stop image core before importing, then restart it")
	}
	if err = s.Migrate(ctx); err != nil {
		return err
	}
	count, err := s.ImportAccounts(ctx, accounts)
	if err != nil {
		return fmt.Errorf("account import failed")
	}
	fmt.Printf("Imported %d accounts; skipped %d duplicates.\n", count, len(accounts)-count)
	return nil
}
