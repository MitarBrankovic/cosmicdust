package cosmicdust

import "strings"

// Labels Docker Compose puts on every container it runs.
const (
	ComposeProjectLabel = "com.docker.compose.project"
	ComposeServiceLabel = "com.docker.compose.service"
)

// AppOf names the application a container belongs to; the control plane keeps
// logs for as long as that application's retention says. Nothing in a runtime
// links a container to an application, so the name comes from, in order:
//
//  1. the labels named by keys, the ones present joined with dots (with keys
//     org, namespace and app this gives "org.namespace.app");
//  2. the Compose project and service, "project.service";
//  3. the image's repository without registry defaults, tag or digest:
//     "docker.io/library/nginx:1.27" and "nginx:1.27" are both "nginx".
//
// It is read once per container, so its cost does not matter; in Parquet the
// column is dictionary-encoded and costs next to nothing per row.
func AppOf(labels map[string]string, image string, keys []string) string {
	var parts []string
	for _, k := range keys {
		if v := labels[k]; v != "" {
			parts = append(parts, v)
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, ".")
	}
	if svc := labels[ComposeServiceLabel]; svc != "" {
		if p := labels[ComposeProjectLabel]; p != "" {
			return p + "." + svc
		}
		return svc
	}
	return imageRepo(image)
}

// imageRepo strips what does not name an application from an image reference.
func imageRepo(ref string) string {
	if i := strings.IndexByte(ref, '@'); i >= 0 {
		ref = ref[:i]
	}
	// A tag follows the last colon after the last slash; a colon before it
	// belongs to a registry port.
	if i := strings.LastIndexByte(ref, ':'); i > strings.LastIndexByte(ref, '/') {
		ref = ref[:i]
	}
	ref = strings.TrimPrefix(ref, "docker.io/")
	ref = strings.TrimPrefix(ref, "library/")
	return ref
}
