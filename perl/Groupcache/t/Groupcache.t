# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Groupcache.t'

#########################

# change 'tests => 2' to 'tests => last_test_to_print';

use strict;
use warnings;
use Data::Dumper;

use Test::More; # tests => 3;
BEGIN { use_ok('Groupcache');
        use_ok('Groupcache::Storage::Mem');
        use_ok('Groupcache::Client');
      };


my $fail = 0;
foreach my $constname (qw()) {
  next if (eval "my \$a = $constname; 1");
  if ($@ =~ /^Your vendor has not defined Groupcache macro $constname/) {
    print "# pass: $@";
  } else {
    print "# fail: $@";
    $fail = 1;
  }

}

ok( $fail == 0 , 'Constants' );
#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my $pid = fork();

if ($pid) {
    my $gc = Groupcache->new(me => "127.0.0.1:4444", storage => Groupcache::Storage::Mem->new());

    $gc->set("test_key1", "test_value1");
    $gc->set("test_key2", "test_value2");
    $gc->set("test_key3", "test_value3");

    is($gc->get("test_key1"), "test_value1");
    is($gc->get("test_key2"), "test_value2");
    is($gc->get("test_key3"), "test_value3");

    wait;

    is($gc->get("test_key4"), "test_value4");

    ok ( !defined $gc->get("test_key2") );
} else {
    my $c = Groupcache::Client->new("localhost:4444"); 
    warn Dumper($c);
    $c->set("test_key4", "test_value4");
    $c->del("test_key2");
    warn "CHILD - test_key1: : " . $c->get("test_key1");
    exit(0);
}

done_testing();
