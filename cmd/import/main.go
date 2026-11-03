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
	spool, err := os.CreateTemp("", "c2go-cli-import-*.json")
	if err != nil {
		return fmt.Errorf("cannot create temporary import file")
	}
	defer os.Remove(spool.Name())
	defer spool.Close()
	n, err := io.Copy(spool, io.LimitReader(reader, 256<<20+1))
	if err != nil || n > 256<<20 {
		return fmt.Errorf("import file exceeds limit or cannot be read")
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
	count, total := 0, 0
	batch := make([]core.Account, 0, 100)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		added, err := s.ImportAccounts(ctx, batch)
		if err != nil {
			return err
		}
		count += added
		batch = batch[:0]
		return nil
	}
	err = core.WalkAccounts(spool, func(a core.Account, parseErr error) error {
		if parseErr != nil {
			return parseErr
		}
		total++
		batch = append(batch, a)
		if len(batch) == 100 {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		return fmt.Errorf("account import failed; completed batches are retained")
	}
	fmt.Printf("Imported %d accounts; reused %d accounts.\n", count, total-count)
	return nil
}
