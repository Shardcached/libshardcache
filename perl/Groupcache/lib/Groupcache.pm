package Groupcache;

use 5.012004;
use strict;
use Carp;

require Exporter;
use AutoLoader;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use Groupcache ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	groupcache_create
	groupcache_del
	groupcache_destroy
	groupcache_get
	groupcache_get_peers
	groupcache_set
	groupcache_test_ownership
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
    croak "&Groupcache::constant not defined" if $constname eq 'constant';
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
XSLoader::load('Groupcache', $VERSION);

sub new {
    my ($class, %params) = @_;
    my $self = {};
    my $me = $params{me};
    if ($me !~ /^[a-z0-9_\.\-]+(:[0-9]+)?$/) {
        croak("'me' MUST be a string in the form 'ADDRESS:PORT'");
    }

    my $storage = $params{storage};
    unless($storage && UNIVERSAL::isa($storage, "Groupcache::Storage")) {
        croak("'storage' MUST be a subclass of Groupache::Storage");
    }

    my $peers = $params{peers} || [];
    if ($peers) {
        unless(ref($peers) && ref($peers) eq 'ARRAY') {
            croak("'peers' MUST be an arrayref of string in the form 'ADDRESS:PORT'");
        }
    } 
    $self->{_storage} = $storage;
    $self->{_peers} = $peers;
    $self->{_gc} = groupcache_create($me, $peers, $storage);
    bless $self, $class;
    return $self;
}

sub set {
    my ($self, $key, $value) = @_;
    return unless $key && $value;
    return groupcache_set($self->{_gc}, $key, length($key), $value, length($value));
}

sub get {
    my ($self, $key) = @_;
    return unless $key;
    return groupcache_get($self->{_gc}, $key, length($key));
}

sub del {
    my ($self, $key) = @_;
    return unless $key;
    return groupcache_del($self->{_gc}, $key, length($key));
}

sub DESTROY {
    my $self = shift;
    groupcache_destroy($self->{_gc})
        if ($self->{_gc})
}

# Preloaded methods go here.

# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__

=head1 NAME

Groupcache - Perl extension for libgroupcache

=head1 SYNOPSIS

  use Groupcache;

    $gc = Groupcache->new(me => "127.0.0.1:4444", storage => Groupcache::Storage::Mem->new());

    $gc->set("test_key1", "test_value1");

    $gc->get("test_key1");

=head1 DESCRIPTION

Bindings to libgroupcache. This module allow to start and manage a groupcache node.
The underlying storage for the node is handled by a Groupcache::Storage subclass. 
Client libraries are also provided in Groupcache::Client 

=head2 EXPORT

None by default.

=head2 Exportable constants

=head2 Exportable functions

  groupcache_t *groupcache_create(char *me,
                        char **peers,
                        int num_peers,
                        groupcache_storage_t *storage)
  int groupcache_del(groupcache_t *cache, void *key, size_t klen)
  void groupcache_destroy(groupcache_t *cache)
  void *groupcache_get(groupcache_t *cache, void *key, size_t klen, size_t *vlen)
  char **groupcache_get_peers(groupcache_t *cache, int *num_peers)
  int groupcache_set(groupcache_t *cache, void *key, size_t klen, void *value, size_t vlen)
  int groupcache_test_ownership(groupcache_t *cache, void *key, size_t len, const char **owner)



=head1 SEE ALSO

=for :list
 * L<Groupcache::Storage>
 * L<Groupcache::Storage::Mem>
 * L<Groupcache::Client>

=head1 AUTHOR

Andrea Guzzo, E<lt>xant@apple.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Andrea Guzzo

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
