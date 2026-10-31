package connpostgres

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

const testTOCList = `;
; Archive created at 2026-10-31 20:29:35 UTC
;
15; 2615 100 SCHEMA - app postgres
20; 1247 101 TYPE app mood postgres
30; 1259 200 TABLE app p postgres
31; 1259 201 TABLE app c postgres
32; 0 0 COMMENT app TABLE c postgres
33; 1259 202 TABLE app u postgres
34; 0 0 COMMENT app TABLE u postgres
35; 1259 203 SEQUENCE app p_id_seq postgres
36; 0 0 SEQUENCE OWNED BY app p_id_seq postgres
37; 2604 204 DEFAULT app p id postgres
38; 1259 205 VIEW app v_c postgres
39; 1259 206 VIEW app v_u postgres
40; 1259 207 VIEW app v_vc postgres
41; 1259 208 TABLE app pt postgres
42; 1259 209 TABLE app pt_1 postgres
43; 0 0 TABLE ATTACH app pt_1 postgres
`

const testTOCPost = `50; 2606 300 CONSTRAINT app p p_pkey postgres
51; 1259 301 INDEX app u_idx postgres
52; 2606 302 FK CONSTRAINT app c c_p_fkey postgres
53; 2606 303 FK CONSTRAINT app c c_u_fkey postgres
54; 0 201 ROW SECURITY app c postgres
55; 3256 304 POLICY app c c_own postgres
56; 3466 305 EVENT TRIGGER - evt postgres
57; 2620 306 TRIGGER app c c_trg postgres
`

const testTOCVerbose = `--
-- TOC entry 15 (class 1 OID 1)
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 20 (class 1 OID 1)
-- Dependencies: 15
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 30 (class 1 OID 1)
-- Dependencies: 15
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 31 (class 1 OID 1)
-- Dependencies: 15 20
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 32 (class 0 OID 0)
-- Dependencies: 31
-- Name: x; Type: X; Schema: app; Owner: postgres
--
CREATE FUNCTION app.f() RETURNS text LANGUAGE sql AS $$
-- Dependencies: 30
select 1 $$;
--
-- TOC entry 33 (class 1 OID 1)
-- Dependencies: 15
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 34 (class 0 OID 0)
-- Dependencies: 33
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 35 (class 1 OID 1)
-- Dependencies: 30 15
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 36 (class 0 OID 0)
-- Dependencies: 35
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 37 (class 1 OID 1)
-- Dependencies: 30 35
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 38 (class 1 OID 1)
-- Dependencies: 31
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 39 (class 1 OID 1)
-- Dependencies: 33
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 40 (class 1 OID 1)
-- Dependencies: 38
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 41 (class 1 OID 1)
-- Dependencies: 15
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 42 (class 1 OID 1)
-- Dependencies: 41 15
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 43 (class 0 OID 0)
-- Dependencies: 42 41
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 50 (class 1 OID 1)
-- Dependencies: 30
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 51 (class 1 OID 1)
-- Dependencies: 33
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 52 (class 1 OID 1)
-- Dependencies: 50 31 30
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 53 (class 1 OID 1)
-- Dependencies: 31 33
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 54 (class 0 OID 201)
-- Dependencies: 31
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 55 (class 1 OID 1)
-- Dependencies: 31 31
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 56 (class 1 OID 1)
-- Name: x; Type: X; Schema: app; Owner: postgres
--
--
-- TOC entry 57 (class 1 OID 1)
-- Dependencies: 31
-- Name: x; Type: X; Schema: app; Owner: postgres
--
`

func rel(name string) objAddr { return objAddr{Type: "table", Names: []string{"app", name}} }

func testPlanInput(t *testing.T) planInput {
	t.Helper()
	pre, err := parseTOCList(strings.NewReader(testTOCList), false)
	require.NoError(t, err)
	post, err := parseTOCList(strings.NewReader(testTOCPost), true)
	require.NoError(t, err)
	deps, err := parseTOCDeps(strings.NewReader(testTOCVerbose))
	require.NoError(t, err)
	entries := slices.Concat(pre, post)
	for i := range entries {
		entries[i].deps = deps[entries[i].id]
	}
	a := func(typ string, names ...string) objAddr { return objAddr{Type: typ, Names: names} }
	info := map[int]objInfo{
		15: {addr: a("schema", "app")},
		20: {addr: a("type", "app.mood")},
		30: {addr: rel("p")},
		31: {addr: rel("c")},
		33: {addr: rel("u")},
		35: {addr: a("sequence", "app", "p_id_seq")},
		37: {addr: a("default value", "app", "p", "id")},
		38: {addr: a("view", "app", "v_c")},
		39: {addr: a("view", "app", "v_u")},
		40: {addr: a("view", "app", "v_vc")},
		41: {addr: rel("pt")},
		42: {addr: rel("pt_1")},
		50: {addr: a("table constraint", "app", "p", "p_pkey")},
		51: {addr: a("index", "app", "u_idx")},
		52: {addr: a("table constraint", "app", "c", "c_p_fkey")},
		53: {addr: a("table constraint", "app", "c", "c_u_fkey")},
		54: {addr: a("row security", "app", "c")},
		55: {addr: a("policy", "app", "c", "c_own"), roles: []objAddr{a("role", "app_user")}},
		56: {addr: a("event trigger", "evt")},
		57: {addr: a("trigger", "app", "c", "c_trg")},
	}
	return planInput{
		entries: entries, info: info, exists: map[string]bool{},
		tables: map[uint32]bool{200: true, 201: true, 208: true, 209: true}, pre: true, post: true,
	}
}

func existing(in planInput, ids ...int) map[string]bool {
	m := make(map[string]bool)
	for _, id := range ids {
		m[in.info[id].addr.key()] = true
	}
	return m
}

func TestParseTOC(t *testing.T) {
	in := testPlanInput(t)
	require.Len(t, in.entries, 24)
	require.Equal(t, tocEntry{id: 54, class: 0, oid: 201, desc: "ROW SECURITY app c postgres", post: true, deps: []int{31}}, in.entries[20])
	byID := func(id int) tocEntry {
		return in.entries[slices.IndexFunc(in.entries, func(e tocEntry) bool { return e.id == id })]
	}
	require.Equal(t, []int{31}, byID(32).deps, "a header inside a function body is not an entry")
	require.Equal(t, []int{42, 41}, byID(43).deps)
	require.Equal(t, []int{15}, byID(30).deps)
}

func TestPlanSubsetScope(t *testing.T) {
	plan := planRestore(testPlanInput(t))
	require.Equal(t, []int{15, 20, 30, 31, 32, 35, 36, 37, 38, 40, 41, 42, 43, 50, 54, 57}, plan.restore)
	require.Equal(t, []int{52}, plan.fks, "the FK to the unmirrored table is dropped")
	require.Equal(t, []uint32{200, 201, 208, 209}, plan.created)
	require.Equal(t, []int{55}, plan.held)
}

func TestPlanIdempotent(t *testing.T) {
	in := testPlanInput(t)
	all := make([]int, 0, len(in.info))
	for id := range in.info {
		all = append(all, id)
	}
	in.exists = existing(in, all...)
	in.exists[objAddr{Type: "role", Names: []string{"app_user"}}.key()] = true
	plan := planRestore(in)
	require.Empty(t, plan.restore)
	require.Empty(t, plan.fks)
	require.Empty(t, plan.created)

	delete(in.exists, in.info[54].addr.key())
	in.pre = false
	require.Equal(t, []int{54}, planRestore(in).restore, "RLS is enabled again when only the table exists")
}

func TestPlanTableAddition(t *testing.T) {
	in := testPlanInput(t)
	in.tables = map[uint32]bool{202: true}
	in.exists = existing(in, 15, 20, 30, 31, 35, 37, 38, 40, 50, 52)
	plan := planRestore(in)
	require.Equal(t, []int{33, 34, 39, 51}, plan.restore)
	require.Equal(t, []int{53}, plan.fks, "the FK from an existing table to the added one is added")
}

func TestPlanWholeDatabase(t *testing.T) {
	in := testPlanInput(t)
	in.tables = nil
	plan := planRestore(in)
	require.Len(t, plan.restore, 20)
	require.Equal(t, []int{52, 53}, plan.fks)
}

func TestDeferredAddSQL(t *testing.T) {
	fk := deferredCon{rel: "app.c", name: "c_p_fkey", def: "FOREIGN KEY (p) REFERENCES app.p(id)", valid: true}
	require.Equal(t, `ALTER TABLE app.c ADD CONSTRAINT "c_p_fkey" FOREIGN KEY (p) REFERENCES app.p(id) NOT VALID`,
		fk.addSQL(shared.POSTGRES_17))
	fk.parted = true
	require.NotContains(t, fk.addSQL(shared.POSTGRES_17), "NOT VALID", "PG17 has no NOT VALID FKs on partitioned tables")
	require.Contains(t, fk.addSQL(shared.POSTGRES_17+10000), "NOT VALID")
	chk := deferredCon{rel: "app.c", name: "c_chk", def: "CHECK (app.f(n)) NOT VALID"}
	require.Equal(t, `ALTER TABLE app.c ADD CONSTRAINT "c_chk" CHECK (app.f(n)) NOT VALID`, chk.addSQL(shared.POSTGRES_17),
		"not valid on the source stays not valid")
}

func TestPickDeferred(t *testing.T) {
	cons := []deferredCon{
		{name: "fk_in_plan", fk: true, entry: 52},
		{name: "fk_out_of_scope", fk: true, entry: 53},
		{name: "chk_missing", addr: objAddr{Type: "table constraint", Names: []string{"app", "c", "chk_missing"}}},
		{name: "chk_present", addr: objAddr{Type: "table constraint", Names: []string{"app", "c", "chk_present"}}},
	}
	exists := map[string]bool{cons[3].addr.key(): true}
	names := func(cs []deferredCon) []string {
		n := make([]string, 0, len(cs))
		for i := range cs {
			n = append(n, cs[i].name)
		}
		return n
	}
	plan := schemaPlan{fks: []int{52}}
	require.Equal(t, []string{"fk_in_plan", "chk_missing"}, names(pickDeferred(plan, cons, exists)))
}
