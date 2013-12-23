package Shardcache::Client::Fast;

use 5.010000;
use strict;
use warnings;
use Carp;

require Exporter;
use AutoLoader;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use Shardcache::Client::Fast ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	shardcache_client_create
	shardcache_client_del
	shardcache_client_destroy
	shardcache_client_evict
	shardcache_client_get
	shardcache_client_set
        shardcache_client_errno
        shardcache_client_errstr
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
    croak "&Shardcache::Client::Fast::constant not defined" if $constname eq 'constant';
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
XSLoader::load('Shardcache::Client::Fast', $VERSION);

# Preloaded methods go here.

# Autoload methods go after =cut, and are processed by the autosplit program.

sub new {
    my ($class, $nodes, $secret) = @_;

    unless($nodes && ref($nodes) && ref($nodes) eq 'ARRAY') {
        croak("'nodes' MUST be an arrayref of string in the form 'ADDRESS:PORT'");
    }

    $secret = '' unless defined $secret;

    my $self = {
        _secret => $secret,
        _nodes => $nodes,
        _client => shardcache_client_create($nodes, $secret)
    };
    bless $self, $class;

    return $self;
}

sub get {
    my ($self, $key) = @_;
    my $val =  shardcache_client_get($self->{_client}, $key);
    if ($val) {
        undef($self->{_errstr});
        $self->{_errno} = 0;
    } else {
        $self->{_errstr} = shardcache_client_errstr($self->{_client});
        $self->{_errno} = shardcache_client_errno($self->{_client});
    }
    return $val;
}

sub set {
    my ($self, $key, $value, $expire) = @_;
    $expire = 0 unless defined $expire;
    my $ret = shardcache_client_set($self->{_client}, $key, $value, $expire);
    if ($ret == 0) {
        undef($self->{_errstr});
        $self->{_errno} = 0;
    } else {
        $self->{_errstr} = shardcache_client_errstr($self->{_client});
        $self->{_errno} = shardcache_client_errno($self->{_client});
    }
    return $ret;
}

sub del {
    my ($self, $key) = @_;
    my $ret = shardcache_client_del($self->{_client}, $key);
    if ($ret == 0) {
        undef($self->{_errstr});
        $self->{_errno} = 0;
    } else {
        $self->{_errstr} = shardcache_client_errstr($self->{_client});
        $self->{_errno} = shardcache_client_errno($self->{_client});
    }
    return $ret;
}

sub evict {
    my ($self, $key) = @_;
    my $ret = shardcache_client_evict($self->{_client}, $key);
    if ($ret == 0) {
        undef($self->{_errstr});
        $self->{_errno} = 0;
    } else {
        $self->{_errstr} = shardcache_client_errstr($self->{_client});
        $self->{_errno} = shardcache_client_errno($self->{_client});
    }
    return $ret;
}

sub stats {
    my ($self, $peer) = @_;
    
    if ($peer) {
        return shardcache_client_stats($self->{_client}, $peer);
    } else {
        my $out;
        foreach my $node (@{$self->{_nodes}}) {
            $out .= shardcache_client_stats($self->{_client}, $node->[0]);
            $out .= "\n";
        }
        return $out;
    }
}

sub check {
    my ($self, $peer) = @_;
    return unless $peer;
    return shardcache_client_check($self->{_client}, $peer);
}

sub index {
    my ($self, $peer) = @_;
    return unless $peer;
    return shardcache_client_index($self->{_client}, $peer);
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
    shardcache_client_destroy($self->{_client});
}


1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

Shardcache::Client::Fast - Perl extension for blah blah blah

=head1 SYNOPSIS

  use Shardcache::Client::Fast;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for Shardcache::Client::Fast, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.

=head2 Exportable functions

  shardcache_client_t *shardcache_client_create(shardcache_node_t *nodes, int num_nodes, char *auth)
  int shardcache_client_del(shardcache_client_t *c, void *key, size_t klen)
  void shardcache_client_destroy(shardcache_client_t *c)
  int shardcache_client_evict(shardcache_client_t *c, void *key, size_t klen)
  size_t shardcache_client_get(shardcache_client_t *c, void *key, size_t klen, void **data)
  int shardcache_client_set(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire)
  int shardcache_client_errno(shardcache_client_t *c)
  char *shardcache_client_errstr(shardcache_client_t *c)

=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

xant, E<lt>xant@macports.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by xant

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
