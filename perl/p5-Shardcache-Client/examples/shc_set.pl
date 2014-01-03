#!/usr/bin/env perl

use strict;
use warnings;

use Shardcache::Client;
use File::Slurp;
use Getopt::Long;

my $USAGE = <<__USAGE ;
Usage: $0 --key=<key> --input=<input_file> [ --expire=<expire> ] [ --hosts=<hosts> ] [ --secret=<secret> ]
       $0 --key=<key> --value=<value> [ --expire=<expire> ] [ --hosts=<hosts> ] [ --secret=<secret> ]
__USAGE

my $infile;
my $key;
my $value;
my $expire;
my $hosts_param = $ENV{SHC_HOSTS};
my $secret = $ENV{SHC_SECRET};


GetOptions ("input=s"  => \$infile,
            "key=s"    => \$key,
            "value=s"  => \$value,
            "hosts=s"  => \$hosts_param,
            "expire=s" => \$expire,
            "secret=s" => \$secret)
            or die("Error in command line arguments\n");


die $USAGE unless ($key && ($value || $infile));

die "SHC_HOSTS environment variable MUST be defined if no --hosts argument is provided\n".
    $USAGE unless($hosts_param);

my @hosts = split(',', $hosts_param);
my $c = Shardcache::Client->new((@hosts > 1) ? \@hosts : $hosts[0], $secret);

if ($infile && !$value) {
    if ($infile eq '-') {
        while(<>) {
            $value .= $_;
        }
    } else {
        $value = read_file($infile);
    }
}
unless ($c->set($key, $value, $expire)) {
    print "Err\n";
    exit(-1);
}

print "Ok\n";
exit(0);
