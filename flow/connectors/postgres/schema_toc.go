package connpostgres

import (
	"bufio"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	pgClassOID        = 1259
	pgPolicyOID       = 3256
	pgEventTriggerOID = 3466
)

type tocEntry struct {
	desc  string
	deps  []int
	id    int
	class uint32
	oid   uint32
	post  bool
}

var (
	tocListRE = regexp.MustCompile(`^(\d+); (\d+) (\d+) (.+)$`)
	tocHeadRE = regexp.MustCompile(`^-- TOC entry (\d+) \(class \d+ OID \d+\)$`)
	tocDepsRE = regexp.MustCompile(`^-- Dependencies: (\d+(?: \d+)*)$`)
)

func newTOCScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	return sc
}

func parseTOCList(r io.Reader, post bool) ([]tocEntry, error) {
	var entries []tocEntry
	sc := newTOCScanner(r)
	for sc.Scan() {
		m := tocListRE.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		id, _ := strconv.Atoi(m[1])
		class, _ := strconv.ParseUint(m[2], 10, 32)
		oid, _ := strconv.ParseUint(m[3], 10, 32)
		entries = append(entries, tocEntry{id: id, class: uint32(class), oid: uint32(oid), desc: m[4], post: post})
	}
	return entries, sc.Err()
}

func parseTOCDeps(r io.Reader) (map[int][]int, error) {
	deps := make(map[int][]int)
	sc := newTOCScanner(r)
	cur := 0
	for sc.Scan() {
		line := sc.Text()
		if m := tocHeadRE.FindStringSubmatch(line); m != nil {
			cur, _ = strconv.Atoi(m[1])
			continue
		}
		if m := tocDepsRE.FindStringSubmatch(line); m != nil && cur != 0 {
			for f := range strings.FieldsSeq(m[1]) {
				d, _ := strconv.Atoi(f)
				deps[cur] = append(deps[cur], d)
			}
		}
		cur = 0
	}
	return deps, sc.Err()
}

type objAddr struct {
	Type  string   `json:"t"`
	Names []string `json:"n,omitempty"`
	Args  []string `json:"a,omitempty"`
}

func (a objAddr) key() string {
	return a.Type + "\x00" + strings.Join(a.Names, "\x00") + "\x01" + strings.Join(a.Args, "\x00")
}

type objInfo struct {
	roles []objAddr
	addr  objAddr
}

type planInput struct {
	info    map[int]objInfo
	exists  map[string]bool
	tables  map[uint32]bool
	entries []tocEntry
	pre     bool
	post    bool
	resync  bool
	cutover bool
}

type schemaPlan struct {
	restore  []int
	fks      []int
	created  []uint32
	held     []int
	matviews []int
}

func (e tocEntry) is(desc string) bool {
	return strings.HasPrefix(e.desc, desc+" ")
}

func (e tocEntry) relation() bool {
	return e.class == pgClassOID && !e.is("INDEX")
}

func (e tocEntry) table() bool {
	return e.class == pgClassOID && e.is("TABLE")
}

func planRestore(in planInput) schemaPlan {
	byID := make(map[int]tocEntry, len(in.entries))
	for _, e := range in.entries {
		byID[e.id] = e
	}
	scope := make(map[int]bool)
	inScope := func(e tocEntry) bool {
		if in.tables == nil {
			return true
		}
		if e.table() && !in.tables[e.oid] {
			return false
		}
		hit, rel := e.table(), false
		for _, d := range e.deps {
			if r, ok := byID[d]; ok && r.relation() {
				rel = true
				if scope[d] {
					hit = true
				} else if !in.exists[in.info[d].addr.key()] {
					return false
				}
			}
		}
		return hit || !rel
	}

	var plan schemaPlan
	picked := make(map[int]bool)
	for _, e := range in.entries {
		if !inScope(e) {
			continue
		}
		if e.relation() {
			scope[e.id] = true
		}
		if e.is("MATERIALIZED VIEW") {
			plan.matviews = append(plan.matviews, e.id)
		}
		info, known := in.info[e.id]
		missing := !in.exists[info.addr.key()] || (in.resync && e.table())
		if !known {
			missing = slices.ContainsFunc(e.deps, func(d int) bool { return picked[d] })
		}
		switch {
		case (e.post && !in.post) || (!e.post && !in.pre) || !missing:
		case e.class == pgEventTriggerOID && !in.cutover:
		case e.class == pgPolicyOID && !allExist(in.exists, info.roles):
			plan.held = append(plan.held, e.id)
		case e.is("FK CONSTRAINT"):
			plan.fks = append(plan.fks, e.id)
		default:
			picked[e.id] = true
			plan.restore = append(plan.restore, e.id)
			if e.table() {
				plan.created = append(plan.created, e.oid)
			}
		}
	}
	return plan
}

func allExist(exists map[string]bool, addrs []objAddr) bool {
	for _, a := range addrs {
		if !exists[a.key()] {
			return false
		}
	}
	return true
}
