package mlsgrid

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Query builds an OData request URL within the API's replication constraints:
// exactly one OriginatingSystemName per request, $filter only on searchable
// fields, no $orderby (results are pre-ordered by ModificationTimestamp),
// paging via @odata.nextLink.
type Query struct {
	// Resource is Property, OpenHouse, etc.
	Resource string
	// OriginatingSystem is the MLS slug; required on every request.
	OriginatingSystem string
	// MlgCanViewTrue adds "MlgCanView eq true" — used by backfill and
	// reconcile. Incremental sync must NOT set it, so revoked records
	// (MlgCanView=false) arrive and can be deleted locally.
	MlgCanViewTrue bool
	// ModificationTimestampGE adds "ModificationTimestamp ge <t>". The
	// comparison is deliberately ge, not gt: paired with idempotent upserts,
	// re-processing boundary records is harmless, whereas gt against a
	// truncated watermark silently skips same-instant records.
	ModificationTimestampGE *time.Time
	// Expand lists child resources (Media, Rooms, UnitTypes). Using $expand
	// caps page size at 1000.
	Expand []string
	// Select restricts returned top-level fields (not supported on expanded
	// resources).
	Select []string
	// Top is $top; 0 omits it (API default 500).
	Top int
}

// timestampFormat matches the API's documented literal form (UTC, sub-second
// precision preserved).
const timestampFormat = "2006-01-02T15:04:05.999Z"

// URL renders the query against a base like DefaultBaseURL.
func (q Query) URL(base string) (string, error) {
	if q.Resource == "" {
		return "", fmt.Errorf("mlsgrid: query needs a resource")
	}
	if q.OriginatingSystem == "" {
		return "", fmt.Errorf("mlsgrid: query needs an originating system — every request must filter exactly one")
	}

	filter := fmt.Sprintf("OriginatingSystemName eq '%s'", q.OriginatingSystem)
	if q.MlgCanViewTrue {
		filter += " and MlgCanView eq true"
	}
	if q.ModificationTimestampGE != nil {
		filter += " and ModificationTimestamp ge " + q.ModificationTimestampGE.UTC().Format(timestampFormat)
	}

	v := url.Values{}
	v.Set("$filter", filter)
	if len(q.Expand) > 0 {
		v.Set("$expand", strings.Join(q.Expand, ","))
	}
	if len(q.Select) > 0 {
		v.Set("$select", strings.Join(q.Select, ","))
	}
	if q.Top > 0 {
		v.Set("$top", strconv.Itoa(q.Top))
	}

	// url.Values encodes spaces as '+'; use %20 — universally accepted in
	// query strings, and what the OData examples show.
	query := strings.ReplaceAll(v.Encode(), "+", "%20")
	return strings.TrimSuffix(base, "/") + "/" + q.Resource + "?" + query, nil
}
