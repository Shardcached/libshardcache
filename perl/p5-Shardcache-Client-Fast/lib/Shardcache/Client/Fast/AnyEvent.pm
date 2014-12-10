package Shardcache::Client::Fast::AnyEvent;

use strict;
use warnings;
use Carp;

use AnyEvent::Handle;

use Shardcache::Client::Fast;
use Shardcache::Client::Fast::MultifResultParser;

our @ISA = qw(Shardcache::Client::Fast);

sub get_multif_ae {
    my ($self, $keys, $options) = @_;
    my $on_error = $options->{on_error}
      or croak 'options->{on_error} is mandatory';
    my $on_result = $options->{on_result}
      or croak 'options->{on_result} is mandatory';

    my $fh = $self->get_multif($keys);
    my $multif_result = Shardcache::Client::Fast::MultifResultParser->new($keys);

    return AnyEvent::Handle->new(
      fh => $fh,
      on_error => sub {
          my ($handle, $fatal, $msg) = @_;
          $handle->destroy;
          return $on_error->($msg);
      },
      on_read => sub {
          my ($handle) = @_;
          my $keys_processed = $multif_result->process_data($handle->{rbuf});
          $keys_processed && ref $keys_processed eq 'HASH'
            or $handle->destroy,
               return $on_error->("failed to process data");
          substr $handle->{rbuf}, 0, length($handle->{rbuf}), '';
          foreach my $key (keys %$keys_processed) {
              $on_result->($key, $keys_processed->{$key});
          }
      });
}

1;

__END__

=head1 NAME

Shardcache::Client::Fast::AnyEvent - AnyEvent flavored Perl extension of Shardcache::Client::Fast

=head1 SYNOPSIS

  use AnyEvent;
  my $cv = AE::cv;

  my $keys = [ 'som', 'keys', 'to', 'retrieve' ];
  $cv->begin foreach @$keys;

  # $handle has to be kept until everything is fetched
  my $handle = $c->get_multif_ae( $keys, {
      on_error => sub {
          my ($msg) = @_;
          return $cv->croak($msg);
      },
      on_result => sub {
          my ($key, $value) = @_;
          print "got $key\n";
          print "got value : $value\n";
          $cv->end();
      },
  } );
  
  $cv->recv;

  # $handle can go out of scope now

=head1 DESCRIPTION

This module inherits of L<Shardcache::Client::Fast::AnyEvent>, and provides
additional methods that are to be used in AnyEvent envorinment

=head1 METHODS

=head2 get_multif_ae( $keys, $options)

Triggers a C<get_multif> call and returns an L<AnyEvent::Handle>, properly
setup to trigger callbacks when receiving keys from shardcached. B<WARNING>,
the return value must stay alive until you have fetched everything you wanted.
Once it's destroyed (by being undef'ed or going out of scope), no more
keys/values will be received from shardcached.

Parameters are:


=head3 $keys

an ArrayRef of keys to get

=head3 $options

a HashRef of:

=over

=item on_error => $cb($error_message)

Mandatory, a CodeRef that will be called in case of error. Will receive the
error message

=item on_result => $cb($key, $value)

Mandatory, a CodeRef that will be called when receiving a result. Will receive the
key and the value

=back


=cut
