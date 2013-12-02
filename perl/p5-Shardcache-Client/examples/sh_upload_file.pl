#!/usr/bin/perl

use strict;

use Shardcache::Client;
use File::Slurp;

my $file = shift @ARGV;
my $destname = shift @ARGV;
my $host = shift @ARGV;

my $data = read_file($file);
$c = Shardcache::Client->new($host);
$c->set($destname, $data);


