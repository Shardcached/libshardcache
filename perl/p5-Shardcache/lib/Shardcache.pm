package Shardcache;

use 5.008005;
use strict;
use warnings;
use Carp;

require Exporter;
use AutoLoader;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use Shardcache ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	shardcache_create
	shardcache_del
	shardcache_destroy
	shardcache_get
	shardcache_get_peers
	shardcache_set
        shardcache_evict
	shardcache_test_ownership
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
);

our $VERSION = '0.01';

sub AUTOLOAD {
    # This AUTOLOAD is used to 'autoload' constants from the constant()
    # XS function.

    my $constname;
    our $AUTOLOAD;
    ($constname = $AUTOLOAD) =~ s/.*:://;
    croak "&Shardcache::constant not defined" if $constname eq 'constant';
    my ($error, $val) = constant($constname);
    if ($error) { croak $error; }
    {
	no strict 'refs';
	# Fixed between 5.005_53 and 5.005_61
#XXX	if ($] >= 5.00561) {
#XXX	    *$AUTOLOAD = sub () { $val };
#XXX	}
#XXX	else {
	    *$AUTOLOAD = sub { $val };
#XXX	}
    }
    goto &$AUTOLOAD;
}

require XSLoader;
XSLoader::load('Shardcache', $VERSION);

sub new {
    my ($class, %params) = @_;
    my $self = {};
    my $me = $params{me};
    if ($me !~ /^[a-z0-9_\.\-]+(:[0-9]+)?$/) {
        croak("'me' MUST be a string in the form 'ADDRESS:PORT'");
    }

    my $storage = $params{storage};
    unless($storage && UNIVERSAL::isa($storage, "Shardcache::Storage")) {
        croak("'storage' MUST be a subclass of Shardache::Storage");
    }

    my $peers = $params{peers} || [];
    if ($peers) {
        unless(ref($peers) && ref($peers) eq 'ARRAY') {
            croak("'peers' MUST be an arrayref of string in the form 'ADDRESS:PORT'");
        }
    } 

    my $secret = $params{secret} || 'default';

    my $num_workers = $params{num_workers} || 10;

    $self->{_storage} = $storage;
    $self->{_peers} = $peers;
    $self->{_me} = $me;
    $self->{_secret} = $secret;
    $self->{_gc} = shardcache_create($me, $peers, $storage, $secret, $num_workers);
    return unless ($self->{_gc});
    bless $self, $class;
    return $self;
}

sub set {
    my ($self, $key, $value) = @_;
    return unless $key && $value;
    return shardcache_set($self->{_gc}, $key, length($key), $value, length($value));
}

sub get {
    my ($self, $key) = @_;
    return unless $key;
    return shardcache_get($self->{_gc}, $key, length($key));
}

sub del {
    my ($self, $key) = @_;
    return unless $key;
    return shardcache_del($self->{_gc}, $key, length($key));
}

sub evict {
    my ($self, $key) = @_;
    return unless $key;
    return shardcache_evict($self->{_gc}, $key, length($key));
}

sub get_owner {
    my ($self, $key) = @_;
    return shardcache_test_ownership($self->{_gc}, $key, length($key));
}

sub me {
    my ($self) = @_;
    return $self->{_me};
}

sub run {
    my ($self, $coderef, $timeout, $priv) = @_;
    return shardcache_run($coderef, $timeout, $priv);
}

sub DESTROY {
    my $self = shift;
    shardcache_destroy($self->{_gc})
        if ($self->{_gc})
}

# Preloaded methods go here.

# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__

=head1 NAME

Shardcache - Perl extension for libshardcache

=head1 SYNOPSIS

  use Shardcache;

    $gc = Shardcache->new(me => "127.0.0.1:4444",
                          storage => Shardcache::Storage::Mem->new(),
                          secret  => "my_Shardcache_secret",
                          peers => ["127.0.0.2:4443"]
                          );

    # NOTE: 'peers' and 'secret' are optional params
    # if no peers are specified, the node will start in standalone mode
    # if no secret is specified, the string 'default' will be used

    $gc->set("test_key1", "test_value1");

    $gc->get("test_key1");

=head1 DESCRIPTION

Bindings to libshardcache. This module allow to start and manage a shardcache node.
The underlying storage for the node is handled by a Shardcache::Storage subclass. 
Client libraries are also provided in Shardcache::Client 

=head2 EXPORT

None by default.

=head1 METHODS

=over 4

=item * new (%params)

=head3 REQUIRED PARAMS
    
=over 4

L<me>

    A 'address:port' string describing the current node

L<storage>

    A valid Shardcache::Storage subclass, implementing the underlying storage

=back

=head3 OPTIONAL PARAMS

=over

L<peers>

    An arrayref containing the peers in our shardcache 'cloud'

L<secret>

    A secret used to compute the signature used for internal communication.
    If not specified the string 'default' will be used 


=back

=item * get ($key)

    Get the value for $key. 
    If found in the cache it will be returned immediately, 
    if this node is responsible for $key, the underlying storage will be queried for the value,
    otherwise a request to the responsible node in the shardcache 'cloud' will be done to obtain the value
    (and the local cache will be populated)

=item * set ($key, $value)

    Set a new value for $key in the underlying storage

=item * del ($key)

    Remove the value associated to $key from the underlying storage (note the cache of all nodes will be evicted as well)

=item * evict ($key)

    Evict the value associated to $key from the cache (note this will not remove the value from the underlying storage)

=item * get_owner ($key)

    Returns the 'address:port' string identifying the node responsible for $key

=item * me ()

    Returns the 'address:port' string identifying the current node

=item * run($coderef, $timeout, $priv)

    Takes over the runloop (AKA : doesn't return) and keeps running the shardcache daemon.
    A callback ($coderef) is registered and it will be called every $timeout milliseconds.
    $priv will be passed as argumend to the $coderef callback.
    If the callback returns '-1' the loop will be stopped and run() will return.
    NOTE: while run() is being executed (and the runloop is taken over) it will not be possible
    to access the Shardcache object, not even throguh the callback.

    So a pattern like this would be WRONG :

    $gc->run(sub { is(shift->get("test_key1"), undef); return -1; }, 500, $gc);
    
    the call to get() done in the callback will fail because while calling the $coderef callback
    a lock will be hold and it will not be possible to call more shardcache methods until it is released.
    So passing the same Shardcache object instance as $priv argument of the $coderef is not supported.

=back

=head2 Exportable functions

  shardcache_t *shardcache_create(char *me, char **peers, int num_peers, shardcache_storage_t *storage)
  int shardcache_del(shardcache_t *cache, void *key, size_t klen)
  void shardcache_destroy(shardcache_t *cache)
  void *shardcache_get(shardcache_t *cache, void *key, size_t klen, size_t *vlen)
  char **shardcache_get_peers(shardcache_t *cache, int *num_peers)
  int shardcache_set(shardcache_t *cache, void *key, size_t klen, void *value, size_t vlen)
  int shardcache_evict(shardcache_t *cache, void *key, size_t klen);
  int shardcache_test_ownership(shardcache_t *cache, void *key, size_t len, const char **owner)

=head1 SEE ALSO

=for :list
 * L<Shardcache::Storage>
 * L<Shardcache::Storage::Mem>
 * L<Shardcache::Client>

=head1 AUTHOR

Andrea Guzzo, E<lt>xant@xant.net<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Andrea Guzzo

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
