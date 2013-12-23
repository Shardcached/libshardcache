# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Shardcache.t'

#########################

# change 'tests => 2' to 'tests => last_test_to_print';

use strict;
use warnings;
use Data::Dumper;

use Test::More; # tests => 3;
BEGIN { use_ok('Shardcache::Client'); };

eval {
    require Shardcache;
    require Shardcache::Storage::Mem;
    1;
} or do {
    warn "Shardcache is not installed, skipping tests";
    done_testing();
    exit(0);
};

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my $pid = fork();

if ($pid) {
    # we are in the main process ... let's start two shardcache instances
    my $gc = Shardcache->new(me => "localhost:4444",
                             nodes => ["localhost:4443", "localhost:4444"],
                             storage => Shardcache::Storage::Mem->new());

    my $gc2 = Shardcache->new(me => "localhost:4443",
                              nodes => ["localhost:4443", "localhost:4444"],
                              storage => Shardcache::Storage::Mem->new());

    # set some keys on the first one
    $gc->set("test_key1", "test_value1");
    $gc->set("test_key2", "test_value2");
    $gc->set("test_key3", "test_value3");

    # check their existance/value on the second one
    is($gc2->get("test_key1"), "test_value1");
    is($gc2->get("test_key2"), "test_value2");
    is($gc2->get("test_key3"), "test_value3");

    # now let's wait for the child process to finish its job
    wait;

    # we should now find some new keys created by the child process
    foreach my $i (4..24) {
        is($gc->get("test_key$i"), "test_value$i");
    }

    # and test_key2 should have been removed by the child process as well
    ok ( !defined $gc->get("test_key2") );
} else {
    # child process ... connect to the second shardcache instance
    sleep(1);
    my $c = Shardcache::Client->new("localhost:4443");
    warn Dumper($c);
    foreach my $i (4..24) {
        $c->set("test_key$i", "test_value$i");
    }

    $c->del("test_key2");

    if ($c->get("test_key1") ne "test_value1") {
        die "Child process can't get a valid value for key: test_key1";
    }

    #warn $c->sts("localhost:4443");
    is($c->chk("localhost:4443"), 1, "check peer1");
    #warn $c->sts("localhost:4444");
    is($c->chk("localhost:4444"), 1, "check peer2");

    exit(0);
}

done_testing();
