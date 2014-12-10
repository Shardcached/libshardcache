package Shardcache::Client::Fast::MultifResultParser;

use strict;
use warnings;

sub new {
    my ($class, $keys) = @_;

    return bless( {
      keys => $keys,
      _accumulator => '',
    }, $class);

}

sub process_data {
    my ($self, $data) = @_;

    $self->{_accumulator} .= $data;

    my %keys_processed;

    while (1) {
        if ($self->{_pending}) {

            # not enough data: bail out
            length($self->{_accumulator}) >= $self->{_pending}->{size}
              or return \%keys_processed;

            # enough data: delete _pending and create result
            my $pending = delete $self->{_pending};
            my $key = $pending->{key};
            my $size = $pending->{size};
            ($keys_processed{$key}, $self->{_accumulator}) = unpack("a${size}a*", $self->{_accumulator});

        } else {

            # nothing pending. if not enough data, bail out
            length($self->{_accumulator}) >= 8
              or return \%keys_processed;

            # enough data for a new result header
            (my $index, my $size, $self->{_accumulator}) = unpack("NNa*", $self->{_accumulator});
            my $key = $self->{keys}->[$index];
            if (length($self->{_accumulator}) >= $size) {
                ($keys_processed{$key}, $self->{_accumulator}) = unpack("a${size}a*", $self->{_accumulator});
            } else {
                $self->{_pending} = { size => $size, key => $key };
            }
        }
        # reloop
    }
}

1;
