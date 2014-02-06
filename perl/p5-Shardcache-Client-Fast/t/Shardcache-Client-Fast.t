# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Shardcache.t'

#########################

# change 'tests => 2' to 'tests => last_test_to_print';

use strict;
use warnings;
use Data::Dumper;

use Test::More; # tests => 3;
BEGIN { use_ok('Shardcache::Client::Fast') };

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
my $c = Shardcache::Client::Fast->new(\@nodes, $ENV{SHC_SECRET});

# set some keys on the first one
my $rc = $c->set("test_key1", "test_value1");
is($rc, 1, "set(test_key1, test_value1)");
$rc = $c->set("test_key2", "test_value2");
is($rc, 1, "set(test_key2, test_value2)");
$rc = $c->set("test_key3", "test_value3");
is($rc, 1, "set(test_key3, test_value3)");

# check their existance/value on the second one
is($c->get("test_key1"), "test_value1", "get(test_key1) == test_value1");
is($c->get("test_key2"), "test_value2", "get(test_key2) == test_value2");
is($c->get("test_key3"), "test_value3", "get(test_key3) == test_value3");

my %results = $c->set_multi({"test_key101" => "test_value101",
                             "test_key102" => "test_value102",
                             "test_key103" => "test_value103"});
is_deeply(\%results,
          {
            "test_key101" => 1,
            "test_key102" => 1,
            "test_key103" => 1
          },
          "set_multi({
             test_key101 => test_value101,
             test_key102 => test_value102,
             test_key103 => test_value103,
          })");

my @vals = $c->get_multi(["test_key101", "test_key102", "test_key103"]);
is_deeply(\@vals, ["test_value101", "test_value102", "test_value103"],
         "get_multi(test_key101, test_key102, test_key103)");

foreach my $i (4..24) { $c->set("test_key$i", "test_value$i"); }

foreach my $i (4..24) {
    is($c->get("test_key$i"), "test_value$i", "get(test_key$i) == test_value$i");
}

$c->del("test_key2");

ok ( !defined $c->get("test_key2"), "del(test_key2)");

foreach my $h (@nodes) {
    my $label = (split(':', $h))[0];
    is($c->chk($label), 1, "chk($label)");
}

done_testing();
