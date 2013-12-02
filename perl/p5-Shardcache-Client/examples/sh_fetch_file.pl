#!/usr/bin/perl

use strict;

use Shardcache::Client;
use File::Slurp;

my $file = shift @ARGV;
my $destname = shift @ARGV;
my $host = shift @ARGV;

$c = Shardcache::Client->new($host);
my $data = $c->get($destname);
write_file($destname, $data);


