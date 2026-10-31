package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func col(num int32, name, typ string) ColumnDef { return ColumnDef{Num: num, Name: name, Type: typ} }

func strp(s string) *string { return &s }

func tdef(table string, cols ...ColumnDef) *TableDef {
	return &TableDef{Table: table, Schema: "public", Columns: cols, Constraints: map[string]string{}, Indexes: map[string]string{}}
}

func sqls(steps []ddlStep) []string {
	out := make([]string, 0, len(steps))
	for _, s := range steps {
		if s.SQL != "" {
			out = append(out, s.SQL)
		} else {
			out = append(out, "WARN "+s.Warn)
		}
	}
	return out
}

func TestPlanTableNoChange(t *testing.T) {
	def := tdef("public.t", col(1, "id", "integer"), col(2, "a", "text"))
	def.Constraints["t_pkey"] = "PRIMARY KEY (id)"
	def.Indexes["t_a"] = "CREATE INDEX t_a ON public.t USING btree (a)"
	cur := tdef("public.t", col(1, "id", "integer"), col(2, "a", "text"))
	cur.Constraints["t_pkey"] = "PRIMARY KEY (id)"
	cur.Indexes["t_a"] = "CREATE INDEX t_a ON public.t USING btree (a)"
	require.Empty(t, planTable(def, cur, def, nil))
}

func TestPlanTableAddColumnFastDefault(t *testing.T) {
	c := col(3, "c", "character varying(10)")
	c.Missing, c.Default, c.NotNull = strp("x"), strp("'y'::character varying"), true
	def := tdef("public.t", col(1, "id", "integer"), c)
	def.Constraints["t_c_check"] = "CHECK (((c)::text <> ''::text))"
	cur := tdef("public.t", col(1, "id", "integer"))
	prev := tdef("public.t", col(1, "id", "integer"))
	require.Equal(t, []string{
		`ALTER TABLE public.t ADD COLUMN "c" character varying(10) DEFAULT 'x'`,
		`ALTER TABLE public.t ALTER COLUMN "c" SET DEFAULT 'y'::character varying`,
		`ALTER TABLE public.t ALTER COLUMN "c" SET NOT NULL`,
		`ALTER TABLE public.t ADD CONSTRAINT "t_c_check" CHECK (((c)::text <> ''::text))`,
	}, sqls(planTable(def, cur, prev, nil)))
}

func TestPlanTableRewriteDefaultWarns(t *testing.T) {
	c := col(2, "u", "uuid")
	c.Default = strp("gen_random_uuid()")
	steps := sqls(planTable(tdef("public.t", col(1, "id", "integer"), c), tdef("public.t", col(1, "id", "integer")),
		tdef("public.t", col(1, "id", "integer")), nil))
	require.Equal(t, []string{
		`ALTER TABLE public.t ADD COLUMN "u" uuid`,
		"WARN public.t.u: values of existing rows exist only on the source",
		`ALTER TABLE public.t ALTER COLUMN "u" SET DEFAULT gen_random_uuid()`,
	}, steps)
}

func TestPlanTableRenameSwapAndType(t *testing.T) {
	prev := tdef("public.t", col(1, "a", "integer"), col(2, "b", "integer"))
	def := tdef("public.t", col(1, "b", "bigint"), col(2, "a", "integer"))
	cur := tdef("public.t", col(1, "a", "integer"), col(2, "b", "integer"))
	require.Equal(t, []string{
		`ALTER TABLE public.t RENAME COLUMN "a" TO "_peerdb_ren_0"`,
		`ALTER TABLE public.t RENAME COLUMN "b" TO "_peerdb_ren_1"`,
		`ALTER TABLE public.t RENAME COLUMN "_peerdb_ren_0" TO "b"`,
		`ALTER TABLE public.t RENAME COLUMN "_peerdb_ren_1" TO "a"`,
		`ALTER TABLE public.t ALTER COLUMN "b" TYPE bigint USING "b"::bigint`,
	}, sqls(planTable(def, cur, prev, nil)))
}

func TestPlanTableDropAndReAdd(t *testing.T) {
	prev := tdef("public.t", col(1, "id", "integer"), col(2, "c", "integer"))
	c := col(3, "c", "integer")
	c.Missing, c.Default = strp("9"), strp("9")
	def := tdef("public.t", col(1, "id", "integer"), c)
	cur := tdef("public.t", col(1, "id", "integer"), col(2, "c", "integer"))
	cur.Columns[1].Default = strp("1")
	require.Equal(t, []string{
		`ALTER TABLE public.t DROP COLUMN IF EXISTS "c" CASCADE`,
		`ALTER TABLE public.t ADD COLUMN "c" integer DEFAULT '9'`,
		`ALTER TABLE public.t ALTER COLUMN "c" SET DEFAULT 9`,
	}, sqls(planTable(def, cur, prev, nil)))
}

func TestPlanTableLegacyNoAttnums(t *testing.T) {
	cur := tdef("public.t", col(0, "id", "integer"), col(0, "old", "integer"))
	cur.Columns[1].NotNull = true
	def := tdef("public.t", col(1, "id", "integer"), col(2, "new", "integer"))
	require.Equal(t, []string{
		`ALTER TABLE public.t ADD COLUMN "new" integer`,
		`ALTER TABLE public.t ALTER COLUMN "old" DROP NOT NULL`,
	}, sqls(planTable(def, cur, &TableDef{}, nil)))
}

func TestPlanTableConstraintsAndIndexes(t *testing.T) {
	prev := tdef("public.t", col(1, "id", "integer"))
	prev.Constraints["t_a_check"] = "CHECK ((id > 0))"
	prev.Indexes["t_u"] = "CREATE UNIQUE INDEX t_u ON public.t USING btree (id)"
	def := tdef("public.t", col(1, "id", "integer"))
	def.Indexes["t_new"] = "CREATE INDEX t_new ON public.t USING btree (id)"
	cur := tdef("public.t2", col(1, "id", "integer"))
	cur.Constraints["t_a_check"] = "CHECK ((id > 0))"
	cur.Constraints["dst_only"] = "CHECK ((id < 100))"
	cur.Indexes["t_u"] = "CREATE UNIQUE INDEX t_u ON public.t2 USING btree (id)"
	cur.Indexes["dst_ix"] = "CREATE INDEX dst_ix ON public.t2 USING btree (id)"
	require.Equal(t, []string{
		`ALTER TABLE public.t2 DROP CONSTRAINT IF EXISTS "t_a_check"`,
		`DROP INDEX IF EXISTS public."t_u"`,
		`CREATE INDEX t_new ON public.t2 USING btree (id)`,
	}, sqls(planTable(def, cur, prev, nil)))
}

func TestPlanTableUnknownPrevDropsUniqueOnly(t *testing.T) {
	def := tdef("public.t", col(1, "id", "integer"))
	cur := tdef("public.t", col(1, "id", "integer"))
	cur.Indexes["u"] = "CREATE UNIQUE INDEX u ON public.t USING btree (id)"
	cur.Indexes["n"] = "CREATE INDEX n ON public.t USING btree (id)"
	require.Equal(t, []string{`DROP INDEX IF EXISTS public."u"`},
		sqls(planTable(def, cur, &TableDef{Columns: []ColumnDef{col(1, "id", "integer")}}, nil)))
}

func TestPlanTablePartitions(t *testing.T) {
	def := tdef("public.ev", col(1, "id", "integer"))
	def.Parts = [][4]*string{{strp("public.ev_2"), strp("public.ev"), strp("FOR VALUES FROM (10) TO (20)"), nil}}
	cur := tdef("public.ev", col(1, "id", "integer"))
	cur.Parts = [][4]*string{{strp("public.ev_1"), strp("public.ev"), strp("FOR VALUES FROM (0) TO (10)"), nil}}
	require.Equal(t, []string{
		"DO $peerdb$ BEGIN IF to_regclass('public.ev_2') IS NULL THEN EXECUTE " +
			"'CREATE TABLE public.ev_2 PARTITION OF public.ev FOR VALUES FROM (10) TO (20)'; ELSE EXECUTE " +
			"'ALTER TABLE public.ev ATTACH PARTITION public.ev_2 FOR VALUES FROM (10) TO (20)'; END IF; END $peerdb$",
		"ALTER TABLE public.ev DETACH PARTITION public.ev_1",
	}, sqls(planTable(def, cur, def, nil)))
}

func TestPlanTableGeneratedAndIdentity(t *testing.T) {
	g := col(2, "g", "integer")
	g.Gen, g.Default = "s", strp("(id * 2)")
	i := col(3, "i", "bigint")
	i.Ident, i.NotNull = "a", true
	def := tdef("public.t", col(1, "id", "integer"), g, i)
	cur := tdef("public.t", col(1, "id", "integer"))
	require.Equal(t, []string{
		`ALTER TABLE public.t ADD COLUMN "i" bigint`,
		`ALTER TABLE public.t ADD COLUMN "g" integer GENERATED ALWAYS AS ((id * 2)) STORED`,
		"WARN public.t.i: values of existing rows exist only on the source",
		`ALTER TABLE public.t ALTER COLUMN "i" ADD GENERATED BY DEFAULT AS IDENTITY`,
		`ALTER TABLE public.t ALTER COLUMN "i" SET NOT NULL`,
	}, sqls(planTable(def, cur, tdef("public.t", col(1, "id", "integer")), nil)))
	dropped := tdef("public.t", col(1, "id", "integer"), col(2, "g", "integer"))
	cur2 := tdef("public.t", col(1, "id", "integer"), g)
	require.Equal(t, []string{`ALTER TABLE public.t ALTER COLUMN "g" DROP EXPRESSION`},
		sqls(planTable(dropped, cur2, def, nil)))
}

func TestPlanTableExcludedAndDestinationOnly(t *testing.T) {
	def := tdef("public.t", col(1, "id", "integer"), col(2, "secret", "text"))
	cur := tdef("public.t", col(1, "id", "integer"), col(2, "_peerdb_synced_at", "timestamp"))
	require.Empty(t, planTable(def, cur, tdef("public.t", col(1, "id", "integer")), map[string]struct{}{"secret": {}}))
}

func TestPlanTableDestinationRenamedFirst(t *testing.T) {
	prev := tdef("public.t", col(1, "id", "integer"), col(2, "c", "integer"))
	def := tdef("public.t", col(1, "id", "integer"), col(2, "c2", "integer"))
	cur := tdef("public.t", col(1, "id", "integer"), col(2, "c2", "integer"))
	require.Empty(t, planTable(def, cur, prev, nil))
}
