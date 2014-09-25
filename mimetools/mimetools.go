package mimetools

import (
	"fmt"
	"mime"
	"strings"
)

// mimeMatcher will match Accept or Content-Type mime types that include *
type Matcher struct {
	// allOk is true, means any mime type is accepted
	allOk bool

	// exact mime type matches
	exact map[string]bool

	// mime type 'text/*' will cause prefix to hold the string 'text/',
	// and will match any string that starts with 'text/'
	prefix []string

	// for completeness '*/html' will cause '/html' in suffix, and match
	// any string that ends with '/html'
	suffix []string
}

func NewMatcher(mediaTypes []string) (*Matcher, error) {
	mm := &Matcher{
		allOk:  false,
		exact:  make(map[string]bool),
		prefix: []string{},
		suffix: []string{},
	}
	for _, x := range mediaTypes {
		err := mm.AddMediaType(x)
		if err != nil {
			return nil, err
		}
	}

	return mm, nil
}

func (mm *Matcher) DebugPrint() {
	fmt.Printf("allOk: %v\n", mm.allOk)
	fmt.Printf("exact: %v\n", mm.exact)
	fmt.Printf("prefix: %v\n", mm.prefix)
	fmt.Printf("suffix: %v\n", mm.suffix)

}

func (mm *Matcher) AddMediaType(mimeString string) error {
	mediaName, _, err := mime.ParseMediaType(mimeString)
	if err != nil {
		return err
	}
	if mediaName == "*/*" {
		mm.allOk = true
	} else if strings.HasPrefix(mediaName, "*/") {
		mm.suffix = append(mm.suffix, strings.Replace(mediaName, "*", "", 1))

	} else if strings.HasSuffix(mediaName, "/*") {
		mm.prefix = append(mm.prefix, strings.Replace(mediaName, "*", "", 1))
	} else {
		mm.exact[mediaName] = true
	}
	return nil
}

func (mm *Matcher) Match(mimeString string) (bool, error) {
	mediaName, _, err := mime.ParseMediaType(mimeString)
	if err != nil {
		return false, err
	}

	if mm.allOk {
		return true, nil
	}
	if mm.exact[mediaName] {
		return true, nil
	}
	for _, p := range mm.prefix {
		if strings.HasPrefix(mediaName, p) {
			return true, nil
		}
	}
	for _, s := range mm.suffix {
		if strings.HasSuffix(mediaName, s) {
			return true, nil
		}
	}

	return false, nil
}
