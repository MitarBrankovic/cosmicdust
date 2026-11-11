package cosmicdust

import "testing"

func TestAppOf(t *testing.T) {
	keys := []string{"c12s.org", "c12s.namespace", "c12s.app"}
	compose := map[string]string{ComposeProjectLabel: "shop", ComposeServiceLabel: "api"}
	cases := []struct {
		name   string
		labels map[string]string
		image  string
		keys   []string
		want   string
	}{
		{"all keys", map[string]string{"c12s.org": "acme", "c12s.namespace": "prod", "c12s.app": "billing"}, "x", keys, "acme.prod.billing"},
		{"some keys", map[string]string{"c12s.app": "billing"}, "x", keys, "billing"},
		{"keys beat compose", map[string]string{"c12s.app": "billing", ComposeServiceLabel: "api"}, "x", keys, "billing"},
		{"compose", compose, "x", keys, "shop.api"},
		{"compose service only", map[string]string{ComposeServiceLabel: "api"}, "x", nil, "api"},
		{"image", nil, "nginx:1.27", keys, "nginx"},
		{"image, defaults stripped", nil, "docker.io/library/nginx:1.27", nil, "nginx"},
		{"image with digest", nil, "ghcr.io/acme/api@sha256:abc", nil, "ghcr.io/acme/api"},
		{"registry port is not a tag", nil, "registry.local:5000/team/api", nil, "registry.local:5000/team/api"},
		{"registry port and tag", nil, "registry.local:5000/team/api:2", nil, "registry.local:5000/team/api"},
		{"nothing", nil, "", nil, ""},
	}
	for _, c := range cases {
		if got := AppOf(c.labels, c.image, c.keys); got != c.want {
			t.Errorf("%s: AppOf = %q, want %q", c.name, got, c.want)
		}
	}
}
