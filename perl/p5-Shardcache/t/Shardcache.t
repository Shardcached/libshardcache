# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Shardcache.t'

#########################

# change 'tests => 2' to 'tests => last_test_to_print';

use strict;
use warnings;
use Data::Dumper;

use Test::More; # tests => 3;
BEGIN { use_ok('Shardcache');
        use_ok('Shardcache::Storage::Mem');
      };

my $fail = 0;
foreach my $constname (qw()) {
  next if (eval "my \$a = $constname; 1");
  if ($@ =~ /^Your vendor has not defined Shardcache macro $constname/) {
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

sub check_deletion {
    my ($gc, $gc2, $from_owner, @nums) = @_;
    foreach my $i (@nums) {
        my $test_key = "test_key$i";
        if ($from_owner) {
            if ($gc2->get_owner($test_key) eq $gc2->me) {
                $gc2->del($test_key);
            } else {
                $gc->del($test_key);
            }
        } else {
            if ($gc2->get_owner($test_key) ne $gc2->me) {
                $gc2->del($test_key);
            } else {
                $gc->del($test_key);
            }
        }
    }
    foreach my $i (@nums) {
        my $test_key = "test_key$i";
        if ($from_owner) {
            if ($gc2->get_owner($test_key) eq $gc2->me) {
                ok ( !defined $gc->get($test_key) );
            } else {
                ok ( !defined $gc2->get($test_key) );
            }
        } else {
            if ($gc2->get_owner($test_key) ne $gc2->me) {
                ok ( !defined $gc->get($test_key) );
            } else {
                ok ( !defined $gc2->get($test_key) );
            }
        }
    }
}
my $gc_name = "localhost:4444";
my $gc2_name = "localhost:4443";
my $gc = Shardcache->new(me => $gc_name,
                         peers => [$gc2_name],
                         storage => Shardcache::Storage::Mem->new(),
                         num_workers => 5);

my $gc2 = Shardcache->new(me => $gc2_name,
                          peers => [$gc_name],
                          storage => Shardcache::Storage::Mem->new(),
                          num_workers => 5);

# set some keys
foreach my $i (0..20) {
    $gc->set("test_key$i", "test_value$i");
}

# check their existance/value
foreach my $i (0..20) {
    is($gc2->get("test_key$i"), "test_value$i");
    is($gc->get("test_key$i"), "test_value$i");
}




check_deletion($gc, $gc2, 0, 0..20);
check_deletion($gc, $gc2, 1, 0..20);
foreach my $i (10..20) {
    my $test_key = "test_key$i";
}

$gc->set("test_key1", "test_value1");
$gc->set("test_key2", "test_value2");

$gc->run(sub {
            # check accessing the $gc instance from
            # within the callback works
            is(shift->get("test_key1"), "test_value1");

            # and also using a client (if the module is available)
            if (eval "require Shardcache::Client; 1") {
                my $c = Shardcache::Client->new($gc->me);
                is ($c->get("test_key2"), "test_value2");
            }

            return -1;
        }, 500, $gc);

# and outside it still works
is($gc->get("test_key1"), "test_value1");
is($gc2->get("test_key1"), "test_value1");

done_testing();
