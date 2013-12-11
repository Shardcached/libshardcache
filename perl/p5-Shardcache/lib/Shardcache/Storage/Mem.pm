package Shardcache::Storage::Mem;

use strict;
use base('Shardcache::Storage');

sub store {
    my ($self, $key, $value) = @_;
    #warn "$self STORE: $key $value";
    $self->{_map}->{$key} = $value;
}

sub fetch {
    my ($self, $key) = @_;
    #warn "$self FETCH: $key -> $self->{_map}->{$key}";
    return \$self->{_map}->{$key};
}

sub remove {
    my ($self, $key) = @_;
    delete $self->{_map}->{$key};
}

sub exist {
    my ($self, $key) = @_;
    return exists $self->{_map}->{$key};
}

sub count {
    my ($self, $key) = @_;
    return scalar(%{$self->{_map}});
}

sub index {
    my ($self, $key) = @_;
    return map +{ key => $_, size => length($self->{_map}->{$_}) }, keys %{$self->{_map}};
}


1;
