package Shardcache::Client::Fast::MultifResult;

use strict;
use warnings;

sub new
{
    my $class = shift;
    my $keys = shift;
    my $fh = shift;

    my $self = bless {}, $class;

    $self->{keys} = $keys;
    $self->{results} = {};
    $self->{fh} = $fh;
    $self->{processed} = 0;

    return $self;
}

sub process_data
{
    my ($self, $data) = @_;

    $self->{accumulator} .= $data;
    if ($self->{pending}) {
        if (length($self->{accumulator}) >= $self->{pending}->{size}) {
            my $index = $self->{pending}->{index};
            my $size = $self->{pending}->{size};
            ($self->{results}->{$self->{keys}->[$index]}, $self->{accumulator}) = unpack("a${size}a*", $self->{accumulator});
            $self->{processed}++;
        }
    } else {
        if (length($self->{accumulator}) > 8) {
            (my $index, my $size, $self->{accumulator}) = unpack("NNa*", $self->{accumulator});
            if ($size == 0) {
                $self->{processed}++;
            } elsif (length($self->{accumulator}) >= $size) {
                ($self->{results}->{$self->{keys}->[$index]}, $self->{accumulator}) = unpack("a${size}a*", $self->{accumulator});
                $self->{processed}++;
            } else {
                $self->{pending} = { size => $size, index => $index };
            }
        }
    }
}

sub read_data
{
    my $self = shift;
    my $data;

    my $rb = read($self->{fh}, $data, 1024);
    return $rb unless ($rb && $rb > 0);

    $self->process_data($data);
}

sub results
{
    my $self = shift;
    return $self->{results};
}

sub is_complete
{
    my $self = shift;
    return $self->{processed} == scalar(@{$self->{keys}});
}

1;
