package core

import (
	"context"
	"fmt"
	"time"
)

// 首次入库时间由数据库维护，与账号创建时间、凭据刷新和来源文件字段无关。
// 历史数据没有可靠时间时保持 NULL，不用更新时间猜测。
const accountImportTimeSchema = `
ALTER TABLE core_accounts ADD COLUMN IF NOT EXISTS imported_at timestamptz;
ALTER TABLE core_accounts ALTER COLUMN imported_at SET DEFAULT now();
CREATE INDEX IF NOT EXISTS core_accounts_imported_page ON core_accounts(imported_at,id);
CREATE INDEX IF NOT EXISTS core_accounts_group_imported_page ON core_accounts(group_id,imported_at,id);
`

type accountImportDates struct {
	ImportedFrom string `json:"imported_from"`
	ImportedTo   string `json:"imported_to"`
}
type accountImportRange struct{ From, To *time.Time }
type accountImportRangeKey struct{}

func parseAccountImportRange(from, to string) (accountImportRange, error) {
	var span accountImportRange
	for _, item := range []struct {
		value string
		end   bool
	}{{from, false}, {to, true}} {
		if item.value == "" {
			continue
		}
		parsed, err := time.ParseInLocation("2006-01-02", item.value, chinaTime)
		if err != nil {
			return span, Err(400, "invalid_import_date", "导入日期无效，请使用 YYYY-MM-DD")
		}
		if item.end {
			parsed = parsed.AddDate(0, 0, 1)
			span.To = &parsed
		} else {
			span.From = &parsed
		}
	}
	if span.From != nil && span.To != nil && !span.To.After(*span.From) {
		return span, Err(400, "invalid_import_date_range", "导入结束日期不能早于开始日期")
	}
	return span, nil
}
func withAccountImportRange(ctx context.Context, span accountImportRange) context.Context {
	return context.WithValue(ctx, accountImportRangeKey{}, span)
}
func selectedAccountImportRange(ctx context.Context) accountImportRange {
	span, _ := ctx.Value(accountImportRangeKey{}).(accountImportRange)
	return span
}
func withAccountQuery(ctx context.Context, q consoleQuery) context.Context {
	return withAccountImportRange(withSelectionGroup(ctx, q.GroupID), accountImportRange{q.ImportedFrom, q.ImportedTo})
}

// 只拼接实际生效的日期条件，使用独立时间列及参数，避免 JSON 转换和全量内存过滤。
func accountImportSQL(span accountImportRange, args *[]any) string {
	clause := ""
	if span.From != nil {
		*args = append(*args, *span.From)
		clause += fmt.Sprintf(" AND imported_at >= $%d", len(*args))
	}
	if span.To != nil {
		*args = append(*args, *span.To)
		clause += fmt.Sprintf(" AND imported_at < $%d", len(*args))
	}
	return clause
}
