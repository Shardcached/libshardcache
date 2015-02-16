package Shardcache::Client::Fast;

use v5.10;
use strict;
use warnings;
use Carp qw(croak);
use Exporter;
use AutoLoader;

our @ISA = qw(Exporter);

our %EXPORT_TAGS = ( 'all' => [ qw(
        shardcache_client_create
        shardcache_client_del
        shardcache_client_destroy
        shardcache_client_evict
        shardcache_client_get
        shardcache_client_getf
        shardcache_client_get_async
        shardcache_client_touch
        shardcache_client_exists
        shardcache_client_set
        shardcache_client_add
        shardcache_client_check
        shardcache_client_index
        shardcache_client_stats
        shardcache_client_errno
        shardcache_client_errstr
        shardcache_client_tcp_timeout
        shardcache_client_check_connection_timeout
        shardcache_client_multi_command_max_wait
        shardcache_client_use_random_node
        shardcache_client_pipeline_max
        shardcache_client_current_node
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT;

our $VERSION = '0.21';

sub AUTOLOAD {
    # This AUTOLOAD is used to 'autoload' constants from the constant()
    # XS function.

    my $constname;
    our $AUTOLOAD;
    ($constname = $AUTOLOAD) =~ s/.*:://;
    croak "&Shardcache::Client::Fast::constant not defined" if $constname eq 'constant';
    my ($error, $val) = constant($constname);
    if ($error) { croak $error; }
    {
        no strict 'refs';
        *$AUTOLOAD = sub { $val };
    }
    goto &$AUTOLOAD;
}

use XSLoader;
XSLoader::load('Shardcache::Client::Fast', $VERSION);

sub new {
    my ($class, $nodes, $log_level) = @_;

    my $self = {
        _nodes  => [],
    };

    if (ref($nodes) && ref($nodes) eq "ARRAY") {
        foreach my $h (@$nodes) {
            my $label;
            my $addr;
            if (ref($h) && ref($h) eq "ARRAY") {
                $label = $h->[0];
                $addr = $h->[1];
            } else {
                if ($h !~ /^[a-zA-Z0-9_\.]+:[a-zA-Z0-9\-\.]+(:[0-9]+)?$/) {
                    die "Invalid host string '$h'";
                }
                ($label, $addr, my $port) = split(':', $h);
                $addr = join(':', $addr, $port);
            }
            push(@{$self->{_nodes}}, [ $label, $addr ]);
        }
    } else {
        if (!$nodes) {
            die "Empty host string";
        }
        elsif ($nodes !~ /^[a-zA-Z0-9_\.]+:[a-zA-Z0-9\-\.]+(:[0-9]+)?$/) {
            die "Invalid host string '$nodes'";
        }
        my ($label, $addr, $port) = split(':', $nodes);
        if ($port) {
            $addr = join(':', $addr, $port);
        } else {
            $label = join(':', $addr,$port);
            $addr = $label;
        }
        push(@{$self->{_nodes}}, [ $label, $addr ]);
    }

    $self->{_client} = shardcache_client_create($self->{_nodes}, $log_level // 0);

    return undef unless ($self->{_client});

    bless $self, $class;

    return $self;
}

sub tcp_timeout {
    my ($self, $new_value) = @_;
    return shardcache_client_tcp_timeout($self->{_client}, $new_value);
}

sub check_connection_timeout {
    my ($self, $new_value) = @_;
    return shardcache_client_check_connection_timeout($self->{_client}, $new_value);
}

sub multi_command_max_wait {
    my ($self, $new_value) = @_;
    return shardcache_client_multi_command_max_wait($self->{_client}, $new_value);
}

sub use_random_node {
    my ($self, $new_value) = @_;
    return shardcache_client_use_random_node($self->{_client}, $new_value);
}

sub pipeline_max {
    my ($self, $new_value) = @_;
    return shardcache_client_pipeline_max($self->{_client}, $new_value);
}

sub current_node {
    my ($self) = @_;
    return shardcache_client_current_node($self->{_client});
}

sub _clear_err {
    my ($self, $val) = @_;
    if ($val) {
        delete $self->{_errstr};
        $self->{_errno} = 0;
    } else {
        $self->{_errstr} = shardcache_client_errstr($self->{_client});
        $self->{_errno} = shardcache_client_errno($self->{_client});
    }
}

sub get {
    my ($self, $key) = @_;
    my $val = shardcache_client_get($self->{_client}, $key);
    $self->_clear_err($val);
    return $val;
}

sub getf {
    my ($self, $key) = @_;
    my $fd = shardcache_client_getf($self->{_client}, $key);
    my $fh;
    open($fh, "<&=", $fd);
    return $fh;
}

sub offset {
    my ($self, $key, $offset, $length) = @_;
    my $val =  shardcache_client_offset($self->{_client}, $key, $offset, $length);
    $self->_clear_err($val);
    return $val;
}

sub get_async {
    my ($self, $key, $cb, $priv) = @_;
    return shardcache_client_get_async($self->{_client}, $key, $cb, $priv);
}

sub set {
    my ($self, $key, $value, $expire) = @_;
    $expire = 0 unless defined $expire;
    my $val = shardcache_client_set($self->{_client}, $key, $value, $expire) == 0;
    $self->_clear_err($val);
    return $val;
}

sub add {
    my ($self, $key, $value, $expire) = @_;
    $expire = 0 unless defined $expire;
    my $val = shardcache_client_add($self->{_client}, $key, $value, $expire) == 0;
    $self->_clear_err($val);
    return $val;
}

sub exists {
    my ($self, $key) = @_;
    my $ret = shardcache_client_exists($self->{_client}, $key);
    $self->_clear_err($ret == 1 || $ret == 0);
    return $ret;
}

sub touch {
    my ($self, $key) = @_;
    my $val = shardcache_client_touch($self->{_client}, $key) == 0;
    $self->_clear_err($val);
    return $val;
}

sub del {
    my ($self, $key) = @_;
    my $val = shardcache_client_del($self->{_client}, $key) == 0;
    $self->_clear_err($val);
    return $val;
}

sub evict {
    my ($self, $key) = @_;
    my $val = shardcache_client_evict($self->{_client}, $key) == 0;
    $self->_clear_err($val);
    return $val;
}

sub stats {
    my ($self, $node) = @_;

    if ($node) {
        return shardcache_client_stats($self->{_client}, $node);
    }

    my $out;
    foreach my $node (@{$self->{_nodes}}) {
        $out .= shardcache_client_stats($self->{_client}, $node->[0]);
        $out .= "\n";
    }
    return $out;
}

sub check {
    my ($self, $node) = @_;
    return unless $node;
    return (shardcache_client_check($self->{_client}, $node) == 0);
}

sub index {
    my ($self, $node) = @_;
    if ($node) {
        return shardcache_client_index($self->{_client}, $node);
    }

    my $out;
    foreach my $node (@{$self->{_nodes}}) {
         my $index = shardcache_client_index($self->{_client}, $node->[0]);
         if ($index) {
             %$out =  { %$out, %$index };
         } else {
             # TODO - Error messages
         }
    }
    return $out;
}

sub get_multi {
    my ($self, $keys, $results) = @_;
    my $res = shardcache_client_get_multi($self->{_client}, $keys, $results);
    return undef unless $res;
    wantarray ? @$res : $res;
}

sub get_multif {
    my ($self, $keys) = @_;
    my $fd = shardcache_client_get_multif($self->{_client}, $keys);
    my $fh;
    open($fh, "<&=", $fd);
    return $fh;
}

sub set_multi {
    my ($self, $pairs) = @_;
    my $res = shardcache_client_set_multi($self->{_client}, $pairs);
    wantarray ? %$res : $res;
}

sub errno {
    my $self = shift;
    return $self->{_errno};
}

sub errstr {
    my $self = shift;
    return $self->{_errstr};
}

sub DESTROY {
    my $self = shift;
    shardcache_client_destroy($self->{_client})
        if ($self->{_client})
}

1;

__END__

=head1 NAME

Shardcache::Client::Fast - Perl extension for the client part of libshardcache

=head1 SYNOPSIS

  use Shardcache::Client::Fast;
  @hosts = ("peer1:localhost:4444", "peer2:localhost:4445", "peer3:localhost:4446" );
  $c = Shardcache::Client::Fast->new(\@hosts, $log_level);

  # Set a new value for key "key"
  $rc = $c->set("key", "value");
  if ($rc != 0) {
    die "Error setting key 'key' : " . $c->errstr;
  }

  # Read the value back
  $v = $c->get("key");
  if (!$v && $c->errno) {
    die "Error getting value for key 'key' : " . $c->errstr;
  }

  # set the key "key2" and make it expire in 60 seconds
  $c->set("key2", "value2", 60);

  # evict "key"
  $c->evict("key");

  # remove "key2" prematurely
  $c->del("key2");

  
  # check if "peer3" is alive
  if ($c->check("peer3") != 0) {
    warn "peer3 is not responding";
  }

  # get the index of keys existing on "peer2"
  $index = $c->index("peer2");

=head1 DESCRIPTION

Perl bindings to libshardcache-client. This library is a replacement for the pure-perl
Shardcache::Client allowing faster access to shardcache nodes by using libshardcache directly
instead of reimplementing the protocol and handling connections on the perl side.

=head2 EXPORT

None by default.

=head1 METHODS

=over 4

=item * new ( @$hosts, [ $log_level ] )

=back

=head3 REQUIRED PARAMS

=over 4

=item @$hosts

    An arrayref of 'name:address:port' strings describing the nodes serving the sharded cache

=back

=head3 OPTIONAL PARAMS

=over

=item $log_level

    The loglevel used by libshardcache internal routines.
    Note that libshardcache uses syslog levels and it initializes
    itself by evaluating the expression : "LOG_INFO + $log_level".
    So anything greater than 0 will enable debug messages (there are up to 5 debug levels);
    negative numbers will progressively inhibit info, notice, warning and error messages.

=back

=over 4

=item * tcp_timeout ( $new_value )

    Set/Get the tcp timeout (in milliseconds) used for all operations on a tcp socket (such as connect(), read(), write()).
    A value of 0 means no-timeout (system-wide timeouts might still apply).
    If $new_value is negative, no new value will be set but the current value will be returned.
    If $new_value is positive it will set as new tcp timeout and the old value will be returned.

=item * use_random_node ( $new_value )

    Set/Get the internal shardcache client flag which determines if using the consistent hashing to
    always query the node responsible for a given key, or select any random node among the available
    ones when executing a get/set/del/evict command.
    If $new_value is true a random node will be used, if false the node responsible for the specific
    key will be selected.
    Note that the 'stats', the 'index' and the 'migration*' commands are not affected by this flag

=item * pipeline_max ( $new_value )

    Set/Get the maximum number of requests to pipeline on a single connection.
    This setting affects how many parallel connections will be used to execute a multi command

=item * get ( $key )

    Get the value for $key. 
    If found in the cache it will be returned immediately, 
    if this node is responsible for $key, the underlying storage will be queried for the value,
    otherwise a request to the responsible node in the shardcache 'cloud' will be done to obtain the value
    (and the local cache will be populated)

=item * get_async ( $key, $coderef, [ $priv ] )

    Get the value for $key asynchronously.

    This function will block and call the provided callback as soon 
    as a chunk of data is read from the node.
    The control will be returned to the caller when there is no
    more data to read or an error occurred

    - $coderef must be a reference to a perl SUB which will get as arguments
      the tuple : ($node, $key, $data, $status, $priv)

    - $status == 0 means that the operation is still in progress and $data contains the new chunk of data

    - $status == 1 means that the operation is complete and no further notifications will be received

    - $status == -1 means that an error occurred and no further notifications will be received

    - $priv will be the same scalar value passed to get_async() as last argument

    If the $coderef, called at each chunk of data being received, returns a 
    NON-TRUE value the fetch will be interrupted and the coderef won't be called
    anymore.
    Returning a TRUE value will make it go ahead until completed.

=item * exists ( $key )

    Check existence of the key on the node responsible for it.
    Returns 1 if the key exists, 0 if doesn't exist, -1 on errors

=item * touch ( $key )

    For loading of a key into the cache if not loaded already,
    otherwise updates the loaded-timestamp for the cached key.

    Returns 0 on success, -1 on errors.

=item * set ( $key, $value, [ $expire ] )

    Set a new value for $key in the underlying storage.

=item * add ( $key, $value, [ $expire ] )

    Set a new value for $key in the underlying storage if it doesn't exists.
    Returns 0 if successfully stored, 1 if already existsing, -1 in case of errors.

=item * del ( $key )

    Remove the value associated to $key from the underlying storage (note the cache of all nodes will be evicted as well).

=item * evict ( $key )

    Evict the value associated to $key from the cache (note this will not remove the value from the underlying storage).

=item * stats ( [ $node ] )

    Retrieve the stats for a given node (or all nodes if none is provided as parameter).

=item * index ( [ $node ] )

    Retrieve the index for a given node (or all nodes if none is provided as parameter).

=item * check ( [ $node ] )

    Checks the status of a given node (or all nodes if none is provided as parameter).

=item * get_multi ( @$keys )

    Get multiple keys at once. The @$keys parameter is expected to be an ARRAYREF containing the keys to retrieve.
    Returns an arrayref containing the values for the requested keys. Values are stored at the same index of the
    corresponding key in the input array. Empty or unretrieved values will be returned as undef.

    Note that multi-commands are not all-or-nothing, some operations may succeed, while others may fail.

=item * set_multi ( %$pairs )

    Get multiple keys at once. The %$pairs parameter is expected to be an HASHREF containing the key/value pairs to set.
    Returns an hashref containing the same keys of the input hashref as keys and the status of the operation as values
    (1 if successfully set, 0 otherwise).

    Note that multi-commands are not all-or-nothing, some operations may succeed, while others may fail.

=back

=head1 SEE ALSO

=over 4

=item * https://github.com/xant/libshardcache

=item * http://xant.github.io/libshardcache/shardcache__client_8h.html

=back

=head1 AUTHOR

xant, E<lt>xant@xant.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by xant

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
