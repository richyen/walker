package test

import (
	//"fmt"
	"github.com/iParadigms/walker/mimetools"
	"testing"
)

func expect(t *testing.T, condition bool, format string, args ...interface{}) {
	if !condition {
		t.Errorf(format, args...)
	}
}

func assert(t *testing.T, condition bool, format string, args ...interface{}) {
	if !condition {
		t.Fatalf(format, args...)
	}
}

func testMatch(t *testing.T, target string, matchInputs ...string) {
	mm, err := mimetools.NewMatcher(matchInputs)
	if err != nil {
		t.Fatal(err)
	}

	q, err := mm.Match(target)
	if err != nil {
		t.Fatal(err)
	}
	if !q {
		t.Fatalf("Failed match against target: %v, inputs: %v", target, matchInputs)
	}
}

func testMatchFail(t *testing.T, target string, matchInputs ...string) {
	mm, err := mimetools.NewMatcher(matchInputs)
	if err != nil {
		t.Fatal(err)
	}

	q, err := mm.Match(target)
	if err != nil {
		t.Fatal(err)
	}
	if q {
		t.Fatalf("Errantly matched against target: %v, inputs: %v", target, matchInputs)
	}
}

func TestGoodMatch(t *testing.T) {
	testMatch(t, "text/html", "text/html")
	testMatch(t, "text/html", "text/*")
	testMatch(t, "text/html", "*/html")

	testMatch(t, "text/html", "foo/bar", "baz/bing", "text/html")
	testMatch(t, "foo/bar", "foo/bar", "baz/bing", "text/html")
	testMatch(t, "baz/bing", "foo/bar", "baz/bing", "text/html")

	testMatch(t, "text/html", "*/*")
	testMatch(t, "foo/bar", "*/*")
	testMatch(t, "baz/bing", "*/*")

	testMatch(t, "text/html; q=1; level=5", "foo/bar; niffler=7", "text/html; q=0.4; level=7")
}

func TestFailMatch(t *testing.T) {
	testMatchFail(t, "foo/bar", "text/html")
	testMatchFail(t, "foo/bar", "text/*")
	testMatchFail(t, "foo/bar", "*/html")
}
