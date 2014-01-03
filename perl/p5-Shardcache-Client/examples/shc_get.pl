#!/usr/bin/env perl

use strict;
use warnings;

use Shardcache::Client;
use File::Slurp;
use Getopt::Long;


my $USAGE = <<__USAGE ;
Usage: $0 --key=<key> [ --output=<output_file> ] [ --hosts=<hosts> ] [ --secret=<secret> ]
__USAGE

my $outfile;
my $key;
my $hosts_param = $ENV{SHC_HOSTS};
my $secret = $ENV{SHC_SECRET};


GetOptions ("output=s" => \$outfile,
            "key=s"    => \$key,
            "hosts=s"  => \$hosts_param,
            "secret=s" => \$secret)
            or die("Error in command line arguments\n");

die $USAGE unless ($key);

die "SHC_HOSTS environment variable MUST be defined if no --hosts argument is provided\n".
    $USAGE unless($hosts_param);

my @hosts = split(',', $hosts_param);

my $c = Shardcache::Client->new((@hosts > 1) ? \@hosts : $hosts[0], $secret);


my $data = $c->get($key);

unless($data) {
    print "No data\n";
    exit(-1);
}

if ($outfile) {
    if ($outfile eq '-') {
        print "$data\n";
    } else {
        write_file($outfile, $data);
        print "OK\n";
    }
} 
exit(0);


