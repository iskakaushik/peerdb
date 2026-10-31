package connpostgres

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type ddlStep struct {
	SQL    string
	Warn   string
	Strict bool
}

func planTable(def, cur, prev *TableDef, exclude map[string]struct{}) []ddlStep {
	var drops, structural, relax, tighten []ddlStep
	dst, q := cur.Table, common.QuoteIdentifier
	alter := func(format string, args ...any) string {
		return "ALTER TABLE " + dst + " " + fmt.Sprintf(format, args...)
	}
	unknown := func(m map[string]string, k string) bool { _, ok := m[k]; return m == nil || ok }

	for name, curDef := range sortedMap(cur.Constraints) {
		if srcDef, ok := def.Constraints[name]; ok && sameDef(srcDef, curDef) || !ok && !unknown(prev.Constraints, name) {
			continue
		}
		drops = append(drops, ddlStep{SQL: alter("DROP CONSTRAINT IF EXISTS %s", q(name)), Strict: true})
	}
	for name, curDef := range sortedMap(cur.Indexes) {
		srcDef, ok := def.Indexes[name]
		if ok && sameDef(onTable(srcDef, def.Table, dst), curDef) ||
			!ok && (!unknown(prev.Indexes, name) || prev.Indexes == nil && !isUnique(curDef)) {
			continue
		}
		drops = append(drops, ddlStep{SQL: "DROP INDEX IF EXISTS " + cur.Schema + "." + q(name), Strict: isUnique(curDef)})
	}

	prevNum := make(map[string]int32, len(prev.Columns))
	for _, c := range prev.Columns {
		if c.Num != 0 {
			prevNum[c.Name] = c.Num
		}
	}
	defByNum := make(map[int32]*ColumnDef, len(def.Columns))
	for i := range def.Columns {
		defByNum[def.Columns[i].Num] = &def.Columns[i]
	}
	target := make(map[string]*ColumnDef, len(cur.Columns))
	claimed := make(map[*ColumnDef]bool, len(def.Columns))
	var renames [][2]string
	for _, dc := range cur.Columns {
		if num := prevNum[dc.Name]; num != 0 {
			if sc := defByNum[num]; sc == nil {
				structural = append(structural, ddlStep{SQL: alter("DROP COLUMN IF EXISTS %s CASCADE", q(dc.Name)), Strict: true})
			} else if target[dc.Name], claimed[sc] = sc, true; sc.Name != dc.Name {
				renames = append(renames, [2]string{dc.Name, sc.Name})
			}
		}
	}
	for _, dc := range cur.Columns {
		if sc := def.column(dc.Name); prevNum[dc.Name] == 0 && sc != nil && !claimed[sc] {
			target[dc.Name], claimed[sc] = sc, true
		} else if prevNum[dc.Name] == 0 && sc == nil && dc.NotNull && len(prevNum) == 0 {
			relax = append(relax, ddlStep{SQL: alter("ALTER COLUMN %s DROP NOT NULL", q(dc.Name)), Strict: true})
		}
	}
	for i, r := range slices.Concat(renames, renames) {
		from, to := r[0], fmt.Sprintf("_peerdb_ren_%d", i%max(len(renames), 1))
		if i >= len(renames) {
			from, to = to, r[1]
		}
		structural = append(structural, ddlStep{SQL: alter("RENAME COLUMN %s TO %s", q(from), q(to)), Strict: true})
	}

	for _, dc := range cur.Columns {
		sc := target[dc.Name]
		if _, excluded := exclude[dc.Name]; sc == nil || excluded {
			continue
		}
		n := q(sc.Name)
		if dc.Gen != "" && sc.Gen == "" {
			structural = append(structural, ddlStep{SQL: alter("ALTER COLUMN %s DROP EXPRESSION", n), Strict: true})
		}
		if dc.Ident != "" && sc.Ident == "" {
			structural = append(structural, ddlStep{SQL: alter("ALTER COLUMN %s DROP IDENTITY IF EXISTS", n), Strict: true})
		}
		if sc.Gen == "" && !sameDef(dc.Type, sc.Type) {
			structural = append(structural, ddlStep{SQL: alter("ALTER COLUMN %s TYPE %s USING %s::%s", n, sc.Type, n, sc.Type), Strict: true})
		}
		if dc.NotNull && !sc.NotNull {
			relax = append(relax, ddlStep{SQL: alter("ALTER COLUMN %s DROP NOT NULL", n), Strict: true})
		}
		tighten = append(tighten, columnTightening(alter, sc, &dc)...)
	}
	for i := range def.Columns {
		sc := &def.Columns[i]
		if _, excluded := exclude[sc.Name]; excluded || claimed[sc] {
			continue
		}
		if sc.Gen != "" {
			tighten = append(tighten, ddlStep{SQL: alter("ADD COLUMN %s %s GENERATED ALWAYS AS (%s) %s", q(sc.Name), sc.Type,
				deref(sc.Default), map[string]string{"s": "STORED", "v": "VIRTUAL"}[sc.Gen])})
			continue
		}
		add, left := alter("ADD COLUMN %s %s", q(sc.Name), sc.Type), &ColumnDef{}
		if sc.Missing != nil {
			lit := utils.QuoteLiteral(*sc.Missing)
			add, left.Default = add+" DEFAULT "+lit, &lit
		} else if sc.Default != nil || sc.Ident != "" {
			tighten = append(tighten, ddlStep{Warn: fmt.Sprintf("%s.%s: values of existing rows exist only on the source", dst, sc.Name)})
		}
		structural = append(structural, ddlStep{SQL: add, Strict: true})
		tighten = append(tighten, columnTightening(alter, sc, left)...)
	}

	for name, srcDef := range sortedMap(def.Constraints) {
		if curDef, ok := cur.Constraints[name]; !ok || !sameDef(srcDef, curDef) {
			tighten = append(tighten, ddlStep{SQL: alter("ADD CONSTRAINT %s %s", q(name), srcDef)})
		}
	}
	for name, srcDef := range sortedMap(def.Indexes) {
		if curDef, ok := cur.Indexes[name]; !ok || !sameDef(onTable(srcDef, def.Table, dst), curDef) {
			tighten = append(tighten, ddlStep{SQL: onTable(srcDef, def.Table, dst)})
		}
	}
	return slices.Concat(drops, structural, relax, planParts(def, cur), tighten)
}

func columnTightening(alter func(string, ...any) string, sc *ColumnDef, dc *ColumnDef) []ddlStep {
	n := common.QuoteIdentifier(sc.Name)
	if sc.Gen != "" && dc.Gen != "" && !sameDef(deref(sc.Default), deref(dc.Default)) {
		return []ddlStep{{SQL: alter("ALTER COLUMN %s SET EXPRESSION AS (%s)", n, deref(sc.Default))}}
	} else if sc.Gen != "" {
		return nil
	}
	var steps []ddlStep
	curDefault := dc.Default
	if dc.Gen != "" {
		curDefault = nil
	}
	if sc.Ident != "" && dc.Ident == "" {
		steps = append(steps, ddlStep{SQL: alter("ALTER COLUMN %s ADD GENERATED BY DEFAULT AS IDENTITY", n)})
	} else if sc.Ident == "" && sc.Default == nil && curDefault != nil {
		steps = append(steps, ddlStep{SQL: alter("ALTER COLUMN %s DROP DEFAULT", n)})
	} else if sc.Ident == "" && sc.Default != nil && !sameDef(*sc.Default, deref(curDefault)) {
		steps = append(steps, ddlStep{SQL: alter("ALTER COLUMN %s SET DEFAULT %s", n, *sc.Default)})
	}
	if sc.NotNull && !dc.NotNull {
		steps = append(steps, ddlStep{SQL: alter("ALTER COLUMN %s SET NOT NULL", n)})
	}
	return steps
}

func planParts(def, cur *TableDef) []ddlStep {
	curParent := make(map[string]string, len(cur.Parts))
	for _, p := range cur.Parts {
		curParent[deref(p[0])] = deref(p[1])
	}
	var steps []ddlStep
	srcChildren := make(map[string]bool, len(def.Parts))
	for _, p := range def.Parts {
		child, parent, bound := deref(p[0]), deref(p[1]), deref(p[2])
		if parent == def.Table {
			parent = cur.Table
		}
		if srcChildren[child] = true; curParent[child] == parent {
			continue
		}
		create := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s %s", child, parent, bound)
		if p[3] != nil {
			create += " PARTITION BY " + *p[3]
		}
		steps = append(steps, ddlStep{SQL: fmt.Sprintf(
			"DO $peerdb$ BEGIN IF to_regclass(%s) IS NULL THEN EXECUTE %s; ELSE EXECUTE %s; END IF; END $peerdb$",
			utils.QuoteLiteral(child), utils.QuoteLiteral(create),
			utils.QuoteLiteral(fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s %s", parent, child, bound))), Strict: true})
	}
	for _, p := range slices.Backward(cur.Parts) {
		if !srcChildren[deref(p[0])] {
			steps = append(steps, ddlStep{SQL: fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s", deref(p[1]), deref(p[0])), Strict: true})
		}
	}
	return steps
}

func onTable(indexDef, src, dst string) string {
	for _, on := range []string{" ON ONLY ", " ON "} {
		if i := strings.Index(indexDef, on+src+" "); i >= 0 {
			return indexDef[:i] + on + dst + indexDef[i+len(on)+len(src):]
		}
	}
	return indexDef
}

func isUnique(indexDef string) bool { return strings.HasPrefix(indexDef, "CREATE UNIQUE ") }

func sameDef(a, b string) bool {
	return strings.Join(strings.Fields(a), " ") == strings.Join(strings.Fields(b), " ")
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func sortedMap(m map[string]string) func(func(string, string) bool) {
	return func(yield func(string, string) bool) {
		for _, k := range slices.Sorted(maps.Keys(m)) {
			if !yield(k, m[k]) {
				return
			}
		}
	}
}
