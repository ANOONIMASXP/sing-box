package build_shared

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/sagernet/sing/common/shell"

	"golang.org/x/mod/semver"
)

const upstreamVersionFile = "release/UPSTREAM_VERSION"

var (
	officialTagPattern    = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|beta|rc)\.[0-9]+)?$`)
	forkSuffixPattern     = regexp.MustCompile(`(?i)-(?:xiaobaf14g|ref1nd)(?:[.-].*)?$`)
	describeSuffixPattern = regexp.MustCompile(`-[0-9]+-g[0-9a-f]+(?:-dirty)?$`)
	dateAnonimaPattern    = regexp.MustCompile(`(?i)\.[0-9]{6}-anonima.*$`)
	plainAnonimaPattern   = regexp.MustCompile(`(?i)-anonima.*$`)
)

// AnonimaVersion appends the UTC build date and the anonima suffix to an
// upstream base version. The returned string intentionally has no leading
// "v"; release tags and workflow tag names add that prefix themselves.
func AnonimaVersion(base string, now time.Time) string {
	base = strings.TrimSpace(base)
	base = strings.TrimPrefix(base, "v")
	return base + "." + now.UTC().Format("060102") + "-anonima"
}

// ReadAnonimaVersion returns <official sing-box version>.<YYMMDD>-anonima.
// The official base is tracked in release/UPSTREAM_VERSION so CI does not
// depend on the fork's remote carrying upstream tags.
func ReadAnonimaVersion() (string, error) {
	base, err := readUpstreamBaseVersion()
	if err != nil {
		return "", err
	}
	return AnonimaVersion(base, time.Now()), nil
}

func readUpstreamBaseVersion() (string, error) {
	if root := projectRoot(); root != "" {
		content, err := os.ReadFile(filepath.Join(root, upstreamVersionFile))
		if err == nil {
			if base := cleanBaseVersion(string(content)); base != "" {
				return base, nil
			}
		}
	}
	return readOfficialTagBase()
}

func projectRoot() string {
	root, err := shell.Exec("git", "rev-parse", "--show-toplevel").ReadOutput()
	if err == nil && strings.TrimSpace(root) != "" {
		return strings.TrimSpace(root)
	}
	workingDirectory, err := os.Getwd()
	if err == nil {
		return workingDirectory
	}
	return ""
}

func readOfficialTagBase() (string, error) {
	tagsOutput, tagsErr := shell.Exec("git", "tag", "--list", "v*").ReadOutput()
	if tagsErr == nil {
		best := ""
		for _, tag := range strings.Split(tagsOutput, "\n") {
			tag = strings.TrimSpace(tag)
			if !officialTagPattern.MatchString(tag) {
				continue
			}
			if best == "" || semver.Compare(tag, best) > 0 {
				best = tag
			}
		}
		if best != "" {
			return strings.TrimPrefix(best, "v"), nil
		}
	}
	described, err := shell.Exec("git", "describe", "--tags", "--abbrev=0").ReadOutput()
	if err != nil {
		return "", fmt.Errorf("read upstream version: %w", err)
	}
	base := cleanBaseVersion(described)
	if base == "" {
		return "", fmt.Errorf("read upstream version: no usable tag in %q", strings.TrimSpace(described))
	}
	return base, nil
}

func cleanBaseVersion(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "v")
	value = dateAnonimaPattern.ReplaceAllString(value, "")
	value = forkSuffixPattern.ReplaceAllString(value, "")
	value = plainAnonimaPattern.ReplaceAllString(value, "")
	value = describeSuffixPattern.ReplaceAllString(value, "")
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, " \t\r\n") {
		return ""
	}
	return value
}
