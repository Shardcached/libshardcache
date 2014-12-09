package Shardcache::Client::Fast::MultifResult;

use strict;
use warnings;

sub new {
    my ($class, $keys, $fh) = @_;

    return bless( {
      keys => $keys,
      results => {},
      fh => $fh,
      _accumulator => '',
    }, $class);

}

sub process_data {
    my ($self, $data) = @_;

    $self->{_accumulator} .= $data;

    my @keys_processed;
    my $results = $self->{results};

    while (1) {
        if ($self->{_pending}) {

            # not enough data: bail out
            length($self->{_accumulator}) >= $self->{_pending}->{size}
              or return \@keys_processed;

            # enough data: delete _pending and create result
            my $pending = delete $self->{_pending};
            my $key = $pending->{key};
            push @keys_processed, $key;
            my $size = $pending->{size};
            ($results->{$key}, $self->{_accumulator}) = unpack("a${size}a*", $self->{_accumulator});

        } else {

            # nothing pending. if not enough data, bail out
            length($self->{_accumulator}) >= 8
              or return \@keys_processed;

            # enough data for a new result header
            (my $index, my $size, $self->{_accumulator}) = unpack("NNa*", $self->{_accumulator});
            my $key = $self->{keys}->[$index];
            if (length($self->{_accumulator}) >= $size) {
                ($results->{$key}, $self->{_accumulator}) = unpack("a${size}a*", $self->{_accumulator});
                push @keys_processed, $key;
            } else {
                $self->{_pending} = { size => $size, key => $key };
            }
        }
        # reloop
    }
}

sub read_data {
    my $self = shift;
    my $data;

    my $rb = read($self->{fh}, $data, 1024);
    return undef unless ($rb && $rb > 0);

    return $self->process_data($data);
}

sub results { $_[0]->{results} }

sub is_complete {
    my ($self) = @_;
    scalar keys %{$self->{results}} == scalar @{$self->{keys}};
}

1;
