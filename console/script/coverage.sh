#!/bin/bash
## This code is here to help me remember the (fairly complicated) invocation of cover I use.
## not this should be run in the walker/console directory, and it will over write the two
## files test/coverage.{out,html}
go test github.com/iParadigms/walker/console/test -cover -coverpkg github.com/iParadigms/walker/console -coverprofile=test/coverage.out && \
go tool cover -html=test/coverage.out -o test/coverage.html

