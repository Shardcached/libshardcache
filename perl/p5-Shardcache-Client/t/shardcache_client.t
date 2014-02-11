# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Shardcache.t'

#########################

# change 'tests => 2' to 'tests => last_test_to_print';

use strict;
use warnings;
use Data::Dumper;

use Test::More; # tests => 3;
BEGIN { use_ok('Shardcache::Client'); };

unless($ENV{SHC_HOSTS}) {
    warn "no SHC_HOTSS defined";
    done_testing();
    exit(0);
}


#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.


# we are in the main process ... let's start two shardcache instances
my @nodes = split(',', $ENV{SHC_HOSTS});
my $c = Shardcache::Client->new(\@nodes, $ENV{SHC_SECRET});

# set some keys on the first one
$c->set("test_key1", "test_value1");
$c->set("test_key2", "test_value2");
$c->set("test_key3", "test_value3");

# check their existance/value on the second one
is($c->get("test_key1"), "test_value1");
is($c->get("test_key2"), "test_value2");
is($c->get("test_key3"), "test_value3");

my %index = $c->index();

cmp_ok(scalar(keys %index), '>', 3);
is ($index{test_key1}, 11);
is ($index{test_key2}, 11);
is ($index{test_key3}, 11);

foreach my $i (4..24) {
    $c->set("test_key$i", "test_value$i");
}

foreach my $i (4..24) {
    is($c->get("test_key$i"), "test_value$i");
}

$c->del("test_key2");

ok ( !defined $c->get("test_key2") );

foreach my $h (@nodes) {
    my $label = (split(':', $h))[0];
    is($c->chk($label), 1, "check $label");
}

is($c->offset("test_key1", 5, 6), "value1");

done_testing();
