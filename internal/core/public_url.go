package core

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// 留空时由每次返回结果的请求决定地址；显式域名默认使用 HTTPS。
func normalizePublicURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	invalid := func() (string, error) {
		return "", fmt.Errorf("CORE_PUBLIC_URL 必须为 HTTP/HTTPS 地址或域名，不允许凭据、查询参数和片段；留空可自动识别")
	}
	if strings.ContainsAny(value, "\\\r\n\t ?#") {
		return invalid()
	}
	if strings.HasPrefix(value, "//") {
		value = "https:" + value
	}
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	u, err := url.Parse(value)
	if err != nil {
		return invalid()
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return invalid()
	}
	if strings.HasSuffix(u.Host, ":") {
		return invalid()
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return invalid()
		}
	}
	return strings.TrimRight(u.String(), "/"), nil
}

func (a *API) imageBaseURL(r *http.Request) string {
	if value := a.Engine.CurrentConfig().PublicURL; value != "" {
		return value
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	// 不信任客户端可自行提供的转发头。反代应保留 Host；HTTPS 终止在反代时可配置公开地址。
	return (&url.URL{Scheme: scheme, Host: r.Host}).String()
}
