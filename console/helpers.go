package console

import ()

const PageWindowLength = 25

func computeDomainPagination(linkPrefix string, dinfos []DomainInfo, windowLength int) []string {
	r := []string{}
	if len(dinfos) == 0 {
		return r
	}
	for i := 0; i < len(dinfos); i = i + windowLength {
		r = append(r, linkPrefix+"/"+dinfos[i].Domain)
	}
	return r
}
