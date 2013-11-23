package Groupcache::Storage::Mem;

use strict;
use base('Groupcache::Storage');

sub store {
    my ($self, $key, $value) = @_;
    #warn "STORE: $key $value";
    $self->{_map}->{$key} = $value;
}

sub fetch {
    my ($self, $key) = @_;
    #warn "FETCH: $key -> $self->{_map}->{$key}";
    return $self->{_map}->{$key};
}

sub remove {
    my ($self, $key) = @_;
    delete $self->{_map}->{$key};
}

1;
