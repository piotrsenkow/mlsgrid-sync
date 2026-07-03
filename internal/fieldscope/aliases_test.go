package fieldscope

import (
	"reflect"
	"testing"
)

func TestCandidatesOrderCanonicalFirst(t *testing.T) {
	m, err := Builtin("mred")
	if err != nil {
		t.Fatal(err)
	}
	got := m.Candidates("PropertyAttachedYN")
	want := []string{"PropertyAttachedYN", "MRD_RENTAL_PROPERTY_TYPE"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Candidates = %v, want %v (newer records must win)", got, want)
	}
}

func TestCandidatesUnaliasedField(t *testing.T) {
	m, _ := Builtin("mred")
	if got := m.Candidates("ListPrice"); !reflect.DeepEqual(got, []string{"ListPrice"}) {
		t.Errorf("unaliased field = %v", got)
	}
}

func TestNilMapIsNoAliasing(t *testing.T) {
	var m *AliasMap
	if got := m.Candidates("ListPrice"); !reflect.DeepEqual(got, []string{"ListPrice"}) {
		t.Errorf("nil AliasMap must pass fields through, got %v", got)
	}
}

func TestBuiltinUnknownSystem(t *testing.T) {
	if _, err := Builtin("nosuch"); err == nil {
		t.Fatal("unknown system must error")
	}
}

func TestLoad(t *testing.T) {
	if m, err := Load(""); err != nil || m != nil {
		t.Errorf("empty spec: got %v, %v; want nil, nil", m, err)
	}
	if m, err := Load("builtin:mred"); err != nil || m == nil {
		t.Errorf("builtin:mred: got %v, %v", m, err)
	}
	if _, err := Load("./aliases.yaml"); err == nil {
		t.Error("custom file spec must error until M8")
	}
}
