Walker
======

An efficient, scalable, continuous crawler leveraging Go/Cassandra

*NOTE*: this project is a work in progress and not ready for production release. Much of the design described below is pending development. Stay tuned for an Alpha release.

# Overview

Walker is a web crawler on it's feet. It has been built from the ground up to be horizontally scalable, smart about recrawling, lean on storage, flexible about what to do with data, and easy to set up. Use it if you:
- Want a broad crawl of the web
- Want to prioritize what you (re)crawl, and how often
- Want control over where you store crawled data and what you use it for (walker stores links and metadat internally, passing pages and files on to you)
- Want a smart crawler that will avoid junk (ex. crawler traps)
- Want the performance of Cassandra and flexibility to do batch processing
- Want to crawl non-html file types
- Aren't interested in built-in web graph generation and search indexing (or want to do it yourself)

# Architecture in brief

Walker takes advantage of Cassandra's distributed nature to store all links it has crawled and still needs to crawl. The database holds these links, all domains we've seen (with metadata), and new segments (groups of links) to crawl for a given domain.

The *fetcher* component claims domains (meaning: fetchers can be distributed to anywhere they can connect to Cassandra), reads in their segments, and crawls pages politely, respecting robots.txt rules. It will parse pages for new links to feed into the system and output crawled content. You can add your own content processor or use a built-in one like writing pages to local files.

The *dispatcher* runs batch jobs looking for domains that don't yet have segments generated, reads the links we already have, and intelligently chooses a subset to crawl next.

# Getting started

## Setup

Make sure you have [go installed and a GOPATH set](https://golang.org/doc/install)

    $ go get github.com/iParadigms/walker

Install and start Cassandra. Simple install on Centos 6 demonstrated below. See the [datastax documentation](http://www.datastax.com/documentation/cassandra/2.0/cassandra/install/install_cassandraTOC.html) non-RHEL-based installs and recommended settings (Oracle Java is recommended but not required)

    echo "[datastax]
    name = DataStax Repo for Apache Cassandra
    baseurl = http://rpm.datastax.com/community
    enabled = 1
    gpgcheck = 0" | sudo tee /etc/yum.repos.d/datastax.repo

    sudo yum install java-1.7.0-openjdk dsc20

    sudo service cassandra start

## Basic crawl

TODO: envision the CLI usage, for example:

    # Crawl a specific site with default refresh intervals
    $GOPATH/bin/walker -domain=<somedomain.com>

## Advanced features and configuration

TODO: add url of documentation site (walker.github.io?)
TODO: document common config items

# Contributing

TODO: mailing list information, contribution rules, source code layout
