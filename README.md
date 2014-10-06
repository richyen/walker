Walker
======

An efficient, scalable, continuous crawler leveraging Go/Cassandra

# Alpha Warning
This project is a work in progress and not ready for production release. Much of the design described below is pending development. Stay tuned for an Alpha release.

# Overview

Walker is a web crawler on it's feet. It has been built from the start to be horizontally scalable, smart about recrawling, lean on storage, flexible about what can be done with data, and easy to set up. Use it if you:
- Want a broad or scalable focused crawl of the web
- Want to prioritize what you (re)crawl, and how often
- Want control over where you store crawled data and what you use it for (walker stores links and metadata internally, passing pages and files on to you)
- Want a smart crawler that will avoid junk (ex. crawler traps)
- Want the performance of Cassandra and flexibility to do batch processing
- Want to crawl non-html file types
- Aren't interested in built-in web graph generation and search indexing (or want to do it yourself)

# Architecture in brief

Walker takes advantage of Cassandra's distributed nature to store all links it has crawled and still needs to crawl. The database holds these links, all domains we've seen (with metadata), and new segments (groups of links) to crawl for a given domain.

The *fetcher manager* component claims domains (meaning: fetchers can be distributed to anywhere they can connect to Cassandra), reads in their segments, and crawls pages politely, respecting robots.txt rules. It will parse pages for new links to feed into the system and output crawled content. You can add your own content processor or use a built-in one like writing pages to local files.

The *dispatcher* runs batch jobs looking for domains that don't yet have segments generated, reads the links we already have, and intelligently chooses a subset to crawl next.

_Note_: the fetchers uses a pluggable *datastore* component to tell it what to crawl (see the `Datastore` interface). Though the Cassandra datastore is the primarily supported implementation, the fetchers could be backed by alternative implementations (in-memory, classic SQL, etc.) that may not need a dispatcher to run at all.

# Getting started

## Setup

Make sure you have [go installed and a GOPATH set](https://golang.org/doc/install):

```sh
go get github.com/iParadigms/walker
```

Make sure the build and basic tests work:

```sh
cd $GOPATH/src/github.com/iParadigms/walker
go test ./test
```

### Running the full test suite

Most Walker tests require dependencies to work and don't run with `go test ./test`. To run the full suite with coverage, use `script/test.sh`.

We use two build tags to enable these tests:

#### sudo

The fetch manager tests, in order to more accurately match what the application does, try to listen locally on port 80. This requires elevated privileges. These use the `sudo` build tag, and `script/test.sh` calls the tests using `sudo -E` to run them.

#### cassandra

The datastore tests require a local Cassandra instance to be running. They automatically set up a `walker_test` keyspace for testing, so shouldn't interfere with existing data (nonetheless running tests with your production Cassandra instance is not a good idea).

A simple install of Cassandra on Centos 6 is demonstrated below. See the [datastax documentation](http://www.datastax.com/documentation/cassandra/2.0/cassandra/install/install_cassandraTOC.html) non-RHEL-based installs and recommended settings (Oracle Java is recommended but not required)

```sh
echo "[datastax]
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/community
enabled = 1
gpgcheck = 0" | sudo tee /etc/yum.repos.d/datastax.repo

sudo yum install java-1.7.0-openjdk dsc20

sudo service cassandra start # it can take a few minutes for this to actually start up
```

Once you have these, the full test suite should work:

```sh
script/test.sh
```

## Basic crawl

Once you've build a `walker` binary, you can crawl with the default handler easily, which simply writes pages to a directory structure in `$PWD`.

```sh
# These assume walker is in your $PATH
walker crawl # start crawling; runs fetchers and a dispatcher all-in-one
walker seed -u http://<test_site>.com # give it a seed URL

# See more help info and other commands:
walker help
```

## Writing your own handler

In most cases you will want to use walker for some kind of processing. The easiest way is to create a new Go project that implements your own handler. You can still take advantage of walker's command-line interface if you don't need to change it. For example:

```go
package main

import "github.com/iParadigms/walker"

type MyHandler struct{}

func (h *MyHandler) HandleResponse(res *walker.FetchResults) {
	// Do something with the response...
}

func main() {
	walker.Cmd.Handler = &MyHandler{}
	walker.Cmd.Execute()
}
```

You can then run walker using your own handler easily:

```sh
go run main.go # Has the same CLI as the walker binary
```

## Advanced features and configuration

### Configuration

See `walker.yaml` for extensive descriptions of the various configuration parameters available for walker. This file is the primary way of configuring your crawl. It is not required to be exist, but will be read if it is in the working directory of the walker process.

One example key is `add_new_domains` (set to false by default) which configures a vertical crawl. It will crawl only the domains of URLs that you specifically seed. Set it to true to branch out to new domains.

Logging can also be configured by having a `log4go.xml` file in the same location as `walker.yaml`. See `walker/log4go.xml.sample` for an example.

TODO: add url of documentation site (walker.github.io?)

# Contributing

TODO: mailing list information, contribution rules, source code layout
