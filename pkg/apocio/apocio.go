// Package apocio is the single owner of APOC's outbound URL and local file
// I/O admission.
//
// APOC loads reach the network and the filesystem from two call paths: the
// native Cypher procedures (apoc.load.json, apoc.load.jsonArray,
// apoc.load.csv, apoc.import.json) and the bundled scalar plugin, whose
// handlers are Go functions in apoc/load. Both paths apply the same policy:
// denied-by-default import file, export file and remote URL access, the
// remote URL allowlist, the address checks (on a URL's resolved host and again
// on the address each dial actually connects to), the local import root, the
// HTTP client defaults (timeout, no redirect following, no ambient proxy,
// dial-time address guard) and the caller's statement context.
//
// The policy is split into two handles that share one immutable record:
//
//   - Policy is the immutable APOC I/O configuration owned by the query
//     executor. Configuration setters replace it with a modified copy; a
//     published Policy is never mutated.
//   - Capability is the typed admission handle required to perform I/O. The
//     executor derives it once per statement from its Policy and binds it in
//     the statement context; standalone Go callers bind one explicitly with
//     WithCapability. A context without a capability performs no I/O: a
//     missing capability never restores unrestricted access.
//
// Both handles are one pointer to that private record, so deriving a
// capability is free and reading it from the statement context copies no
// configuration and allocates nothing.
//
// Example (a standalone Go caller reading an explicitly allowed URL):
//
//	policy := apocio.NewPolicy(apocio.Config{
//		AllowRemoteURLAccess: true,
//		RemoteURLAllowlist:   []string{"api.example.com"},
//	})
//	ctx := apocio.WithCapability(context.Background(), policy.Capability())
//	capability, err := apocio.CapabilityFrom(ctx)
//	if err != nil {
//		return err // no capability, no I/O
//	}
//	body, err := capability.Open(ctx, "https://api.example.com/data.json", apocio.Request{})
//	if err != nil {
//		return err
//	}
//	defer body.Close()
package apocio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/orneryd/nornicdb/pkg/security"
)

var (
	// ErrLocalImportFileAccessDisabled reports a local APOC load while import
	// file access is disabled (the default).
	ErrLocalImportFileAccessDisabled = errors.New("local APOC import file access is disabled")
	// ErrLocalExportFileAccessDisabled reports a local APOC export while export
	// file access is disabled (the default).
	ErrLocalExportFileAccessDisabled = errors.New("local APOC export file access is disabled")
	// ErrRemoteURLAccessDisabled reports an APOC HTTP(S) fetch while remote URL
	// access is disabled (the default).
	ErrRemoteURLAccessDisabled = errors.New("remote APOC URL access is disabled")
	// ErrRemoteURLHostNotAllowed reports a remote APOC URL whose host is not on
	// the configured allowlist. A remote fetch always requires an allowlist.
	ErrRemoteURLHostNotAllowed = errors.New("APOC remote URL host is not allowlisted")
	// ErrFileAuthorityNotAllowed reports a file URL with an authority section.
	ErrFileAuthorityNotAllowed = errors.New("file URL may not contain an authority section (i.e. it should be 'file:///')")
	// ErrFileQueryNotAllowed reports a file URL with a query component.
	ErrFileQueryNotAllowed = errors.New("file URL may not contain a query component")
	// ErrFileFragmentNotAllowed reports a file URL with a fragment component.
	ErrFileFragmentNotAllowed = errors.New("file URL may not contain a fragment component")
	// ErrCapabilityMissing reports I/O attempted without the typed capability.
	// APOC I/O never falls back to unrestricted access in that case.
	ErrCapabilityMissing = errors.New("APOC I/O capability is not bound to this context")
)

// errRemoteAddressDisallowed is the one rejection the remote URL address policy
// reports: for a URL host the preflight resolved, and for the resolved address a
// dial is about to connect to.
var errRemoteAddressDisallowed = errors.New("APOC remote URL host resolves to a disallowed address")

// HostResolver resolves a URL host for the address policy. A nil resolver in
// Config or Policy uses the process resolver.
type HostResolver func(context.Context, string) ([]net.IPAddr, error)

// Config is the configuration a Policy is built from. It is a plain input
// record: the Policy copies what it needs, so later changes to Config or its
// slices do not affect a Policy that was already built.
type Config struct {
	// AllowImportFileAccess permits APOC load/import local file reads.
	AllowImportFileAccess bool
	// AllowExportFileAccess permits APOC export local file writes.
	AllowExportFileAccess bool
	// AllowRemoteURLAccess permits APOC load/import HTTP(S) fetches.
	AllowRemoteURLAccess bool
	// RemoteURLAllowlist restricts APOC HTTP(S) fetches to these hosts. Entries
	// are matched against URL hostnames and may be exact names or wildcard
	// suffixes like "*.example.com".
	RemoteURLAllowlist []string
	// FileAccessRoot is the local import root. When set, local APOC file paths
	// and file URLs are rebased beneath it.
	FileAccessRoot string
	// HTTPClient replaces the default APOC remote HTTP client. A nil client
	// keeps the APOC defaults (10s timeout, no redirect following, no proxy).
	HTTPClient *http.Client
	// HostResolver replaces the process resolver for the URL address policy.
	HostResolver HostResolver
}

// policy is the immutable APOC I/O configuration record. One record is built
// once and replaced — never modified — by the Policy.With* methods, so every
// Capability derived from it keeps exactly the configuration it was derived
// from. It is private because both exported handles are one pointer to it.
type policy struct {
	allowImportFileAccess bool
	allowExportFileAccess bool
	allowRemoteURLAccess  bool
	remoteURLAllowlist    []string
	fileAccessRoot        string
	httpClient            *http.Client
	hostResolver          HostResolver
}

// deniedPolicy is the configuration of the zero Policy: every I/O direction
// denied and the APOC HTTP defaults. It is shared and never modified.
var deniedPolicy = &policy{}

// Policy is the executor's immutable APOC I/O configuration handle.
//
// Every With* method returns a modified copy; no method mutates a published
// Policy or the caller's slices. The zero Policy enables nothing.
type Policy struct {
	record *policy
}

// policyRecord returns the policy record this handle names. The zero Policy names
// the shared denied-by-default record instead of a nil pointer, so no method
// needs a nil check and an unconfigured executor cannot admit I/O.
func (p Policy) policyRecord() *policy {
	if p.record == nil {
		return deniedPolicy
	}
	return p.record
}

// defaultRemoteHTTPClient is the APOC remote client default: a bounded
// timeout, no redirect following (a redirect never becomes an unvalidated
// second target), no ambient proxy and a dialer that refuses a prohibited
// address before the socket connects.
var defaultRemoteHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
	// A non-nil DialContext makes net/http stop enabling HTTP/2 on its own
	// (net/http.Transport.protocols); the flag keeps the HTTP/2 support the
	// default client had before the dial guard existed.
	Transport: &http.Transport{
		Proxy:             nil,
		ForceAttemptHTTP2: true,
		DialContext:       defaultRemoteDialer.DialContext,
	},
}

// defaultRemoteDialer dials the APOC remote client's connections with the same
// settings net/http's default transport uses, so the process resolver, the
// Happy Eyeballs fallback and the caller's context are unchanged.
//
// ControlContext adds the address judgment. It runs after the socket exists and
// before connect(2), on the resolved endpoint Go computed for that attempt
// (netFD.dial passes the resolved remote address), so a name that resolves to a
// prohibited address at dial time never gets a TCP connection even when the URL
// preflight resolved the same name to an allowed one.
var defaultRemoteDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
	ControlContext: func(_ context.Context, _, address string, _ syscall.RawConn) error {
		return checkRemoteDialAddress(address)
	},
}

// checkRemoteDialAddress applies the remote URL address policy to the address a
// dial attempt is about to connect to. The dialer reports an already resolved
// endpoint, so the host is an IP literal here. An empty or unparseable address
// yields no IP and fails closed, because an address that cannot be judged is not
// connected to.
func checkRemoteDialAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("APOC remote dial address %q is not an IP:port: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("APOC remote dial address %q is not a resolved IP address", address)
	}
	if security.IsRestrictedIP(ip, remoteIPPolicy) {
		return errRemoteAddressDisallowed
	}
	return nil
}

// remoteIPPolicy is the APOC remote address contract: the shared
// loopback/private/link-local restriction security.ValidateURL applies to a
// literal URL host, extended with multicast and unspecified addresses, which
// APOC never connects to. The URL preflight and the dial guard both ask
// security.IsRestrictedIP with this policy.
var remoteIPPolicy = security.IPPolicy{RejectMulticast: true, RejectUnspecified: true}

// NewPolicy builds an immutable Policy from cfg. Access is denied unless cfg
// enables it.
func NewPolicy(cfg Config) Policy {
	return Policy{record: &policy{
		allowImportFileAccess: cfg.AllowImportFileAccess,
		allowExportFileAccess: cfg.AllowExportFileAccess,
		allowRemoteURLAccess:  cfg.AllowRemoteURLAccess,
		remoteURLAllowlist:    normalizeRemoteURLAllowlist(cfg.RemoteURLAllowlist),
		fileAccessRoot:        strings.TrimSpace(cfg.FileAccessRoot),
		httpClient:            cfg.HTTPClient,
		hostResolver:          cfg.HostResolver,
	}}
}

// WithImportFileAccess returns the policy with APOC import file access set.
func (p Policy) WithImportFileAccess(enabled bool) Policy {
	next := *p.policyRecord()
	next.allowImportFileAccess = enabled
	return Policy{record: &next}
}

// WithExportFileAccess returns the policy with APOC export file access set.
func (p Policy) WithExportFileAccess(enabled bool) Policy {
	next := *p.policyRecord()
	next.allowExportFileAccess = enabled
	return Policy{record: &next}
}

// WithRemoteURLAccess returns the policy with APOC remote URL access set.
func (p Policy) WithRemoteURLAccess(enabled bool) Policy {
	next := *p.policyRecord()
	next.allowRemoteURLAccess = enabled
	return Policy{record: &next}
}

// WithRemoteURLAllowlist returns the policy with a normalized copy of hosts as
// its remote URL allowlist. Blank entries and duplicates are dropped.
func (p Policy) WithRemoteURLAllowlist(hosts []string) Policy {
	next := *p.policyRecord()
	next.remoteURLAllowlist = normalizeRemoteURLAllowlist(hosts)
	return Policy{record: &next}
}

// WithLocalFileAccessRoot returns the policy with root as its local import
// root. Surrounding whitespace is ignored; an empty root means no rebasing.
func (p Policy) WithLocalFileAccessRoot(root string) Policy {
	next := *p.policyRecord()
	next.fileAccessRoot = strings.TrimSpace(root)
	return Policy{record: &next}
}

// WithHTTPClient returns the policy with client as its remote HTTP client. A
// nil client keeps the APOC defaults.
func (p Policy) WithHTTPClient(client *http.Client) Policy {
	next := *p.policyRecord()
	next.httpClient = client
	return Policy{record: &next}
}

// WithHostResolver returns the policy with resolver as its URL host resolver.
func (p Policy) WithHostResolver(resolver HostResolver) Policy {
	next := *p.policyRecord()
	next.hostResolver = resolver
	return Policy{record: &next}
}

// Capability returns the typed admission handle for this policy. It names the
// same immutable record, so deriving it copies no configuration. Callers that
// are not a query executor bind it with WithCapability.
func (p Policy) Capability() Capability {
	return Capability{record: p.policyRecord()}
}

// Capability is the typed APOC I/O admission handle: one pointer to the
// immutable policy it was derived from. It carries no mutable state of its
// own and no separate "bound" flag; the zero Capability names no policy and
// therefore admits nothing.
type Capability struct {
	record *policy
}

// CapabilityKey is the context key a bound Capability is stored under. The
// query executor's per-statement context answers it directly, so binding costs
// no extra context per row or call.
type CapabilityKey struct{}

// WithCapability returns ctx carrying c. Standalone Go callers use it to bind
// the capability their policy derives; the APOC loaders require it.
//
// The binding always shadows a capability the caller's context already carried.
// An unbound (zero) capability therefore binds an explicit missing capability
// rather than leaving an outer statement's access in place: a zero capability
// can only deny, never inherit.
func WithCapability(ctx context.Context, c Capability) context.Context {
	return context.WithValue(ctx, CapabilityKey{}, c)
}

// CapabilityFrom returns the capability bound in ctx. It reports
// ErrCapabilityMissing when ctx carries none: APOC I/O does not fall back to
// unrestricted access.
func CapabilityFrom(ctx context.Context) (Capability, error) {
	if ctx != nil {
		if c, ok := ctx.Value(CapabilityKey{}).(Capability); ok && c.record != nil {
			return c, nil
		}
	}
	return Capability{}, ErrCapabilityMissing
}

// Request is the outbound request for a remote APOC source. Method defaults to
// GET; Headers and Body apply to remote URLs only. A local source accepts no
// request options.
type Request struct {
	Method  string
	Headers map[string]string
	Body    []byte
}

func (r Request) hasOptions() bool {
	if len(r.Headers) != 0 || len(r.Body) != 0 {
		return true
	}
	method := strings.TrimSpace(r.Method)
	return method != "" && !strings.EqualFold(method, http.MethodGet)
}

func (r Request) method() string {
	if method := strings.TrimSpace(r.Method); method != "" {
		return method
	}
	return http.MethodGet
}

// Open opens source for reading, applying every gate this capability owns.
//
// An HTTP(S) source is validated (URL policy, scheme, host, allowlist, resolved
// addresses) and fetched with ctx, the APOC client defaults and request; a
// response other than 200 is an error. Any other source is a local import file:
// import file access must be allowed, the path is resolved and rebased under the
// configured root, and it is opened through the rooted file owner. A local
// source rejects request options instead of ignoring them.
//
// The caller closes the returned reader.
func (c Capability) Open(ctx context.Context, source string, request Request) (io.ReadCloser, error) {
	if c.record == nil {
		return nil, ErrCapabilityMissing
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if IsHTTPSource(source) {
		return c.openRemote(ctx, source, request)
	}
	if request.hasOptions() {
		return nil, fmt.Errorf("expected local APOC file source, got %s request with options", strings.ToUpper(request.method()))
	}
	resolved, err := c.ImportPath(source)
	if err != nil {
		return nil, err
	}
	file, err := security.OpenRootedFile(resolved, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		file.Close()
		return nil, err
	}
	return &sourceReader{ReadCloser: file, ctx: ctx}, nil
}

// sourceReader preserves the caller lifetime even when a configured transport
// supplies a body that does not observe request cancellation. An in-flight read
// may finish. Its primary failure is preserved; otherwise cancellation discards
// its bytes, and no subsequent read reaches the source.
type sourceReader struct {
	io.ReadCloser
	ctx context.Context
}

func (s *sourceReader) Read(buffer []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := s.ReadCloser.Read(buffer)
	if err != nil && err != io.EOF {
		return n, err
	}
	if canceled := s.ctx.Err(); canceled != nil {
		return 0, canceled
	}
	return n, err
}

func (c Capability) openRemote(ctx context.Context, source string, request Request) (io.ReadCloser, error) {
	if !c.record.allowRemoteURLAccess {
		return nil, ErrRemoteURLAccessDisabled
	}
	parsed, err := c.record.remoteURL(ctx, source)
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if len(request.Body) > 0 {
		body = bytes.NewReader(request.Body)
	}
	req, err := http.NewRequestWithContext(ctx, request.method(), parsed.String(), body)
	if err != nil {
		return nil, err
	}
	for key, value := range request.Headers {
		req.Header.Set(key, value)
	}
	resp, err := c.record.remoteHTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}
	if err := ctx.Err(); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return &sourceReader{ReadCloser: resp.Body, ctx: ctx}, nil
}

// ImportPath resolves a local APOC import source to the path to open: import
// file access must be allowed, file URLs are validated, and the result is
// rebased beneath the configured import root when one is set.
func (c Capability) ImportPath(source string) (string, error) {
	if c.record == nil {
		return "", ErrCapabilityMissing
	}
	return c.localPath(source, ErrLocalImportFileAccessDisabled, c.record.allowImportFileAccess)
}

// ExportPath resolves a local APOC export destination: export file access must
// be allowed, file URLs are validated, and the result is rebased beneath the
// configured import root when one is set.
func (c Capability) ExportPath(source string) (string, error) {
	if c.record == nil {
		return "", ErrCapabilityMissing
	}
	return c.localPath(source, ErrLocalExportFileAccessDisabled, c.record.allowExportFileAccess)
}

func (c Capability) localPath(source string, disabled error, allowed bool) (string, error) {
	if !allowed {
		return "", disabled
	}

	// A native Windows drive path is a local path, not a URL scheme.
	if filepath.IsAbs(source) && filepath.VolumeName(source) != "" {
		return c.record.rebaseImportPath(source)
	}

	parsed, err := url.Parse(source)
	if err == nil && parsed.Scheme != "" {
		switch strings.ToLower(parsed.Scheme) {
		case "file":
			if err := validateFileURL(parsed); err != nil {
				return "", err
			}
			localPath := parsed.Path
			if localPath == "" {
				localPath = parsed.Opaque
			}
			return c.record.rebaseImportPath(localPath)
		case "http", "https":
			return "", fmt.Errorf("expected local APOC file source, got remote URL")
		default:
			return "", fmt.Errorf("unsupported APOC file URL scheme %q", parsed.Scheme)
		}
	}

	return c.record.rebaseImportPath(source)
}

// remoteURL validates rawURL against the remote URL policy and returns it.
func (p *policy) remoteURL(ctx context.Context, rawURL string) (*url.URL, error) {
	if err := security.ValidateURL(rawURL, security.URLPolicy{}); err != nil {
		return nil, fmt.Errorf("invalid APOC remote URL: %w", err)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid APOC remote URL: %w", err)
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return nil, fmt.Errorf("unsupported APOC remote URL scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("APOC remote URL host is required")
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("APOC remote URLs may not contain userinfo")
	}
	if parsed.Fragment != "" {
		return nil, fmt.Errorf("APOC remote URLs may not contain fragments")
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return nil, fmt.Errorf("APOC remote URL host is required")
	}
	if !remoteHostAllowed(hostname, p.remoteURLAllowlist) {
		return nil, ErrRemoteURLHostNotAllowed
	}
	ipList, err := p.lookupHost(ctx, hostname)
	if err != nil {
		return nil, fmt.Errorf("resolve APOC remote host: %w", err)
	}
	for _, ipAddr := range ipList {
		if ip := ipAddr.IP; ip != nil && security.IsRestrictedIP(ip, remoteIPPolicy) {
			return nil, errRemoteAddressDisallowed
		}
	}
	return parsed, nil
}

func (p *policy) lookupHost(ctx context.Context, hostname string) ([]net.IPAddr, error) {
	if p.hostResolver != nil {
		return p.hostResolver(ctx, hostname)
	}
	return net.DefaultResolver.LookupIPAddr(ctx, hostname)
}

func (p *policy) remoteHTTPClient() *http.Client {
	if p.httpClient != nil {
		return p.httpClient
	}
	return defaultRemoteHTTPClient
}

// rebaseImportPath resolves localPath under the configured import root.
func (p *policy) rebaseImportPath(localPath string) (string, error) {
	root := p.fileAccessRoot
	if root == "" {
		return filepath.Clean(localPath), nil
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve APOC import root: %w", err)
	}

	relativePath := normalizeImportRelativePath(localPath)
	target := absRoot
	if relativePath != "" {
		target = filepath.Join(absRoot, filepath.FromSlash(relativePath))
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolve APOC local file path: %w", err)
	}
	if target != absRoot && !strings.HasPrefix(target, absRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("file URL points outside configured import directory")
	}
	return target, nil
}

func validateFileURL(uri *url.URL) error {
	if uri.Host != "" {
		return ErrFileAuthorityNotAllowed
	}
	if uri.RawQuery != "" {
		return ErrFileQueryNotAllowed
	}
	if uri.Fragment != "" {
		return ErrFileFragmentNotAllowed
	}
	return nil
}

func normalizeImportRelativePath(localPath string) string {
	slashPath := strings.ReplaceAll(localPath, "\\", "/")
	cleaned := pathpkg.Clean("/" + strings.TrimLeft(slashPath, "/"))
	return strings.TrimPrefix(cleaned, "/")
}

// normalizeRemoteURLAllowlist lower-cases entries, trims surrounding space and
// drops blanks and duplicates. A nil result means no host is allowed.
func normalizeRemoteURLAllowlist(hosts []string) []string {
	out := make([]string, 0, len(hosts))
	seen := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		normalized := strings.ToLower(strings.TrimSpace(host))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func remoteHostAllowed(hostname string, allowlist []string) bool {
	hostname = strings.ToLower(strings.TrimSpace(hostname))
	if hostname == "" || len(allowlist) == 0 {
		return false
	}
	for _, entry := range allowlist {
		if entry == hostname {
			return true
		}
		if strings.HasPrefix(entry, "*.") {
			suffix := strings.TrimPrefix(entry, "*")
			if strings.HasSuffix(hostname, suffix) && hostname != strings.TrimPrefix(suffix, ".") {
				return true
			}
		}
	}
	return false
}

// IsHTTPSource reports whether source is an HTTP(S) URL rather than a local
// import file. It is the classification the I/O owner and its callers branch on.
func IsHTTPSource(source string) bool {
	return strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://")
}
