package Shardcache::Storage;

use strict;

sub new {
    my ($class) = @_;
    my $self = {};
    bless $self, $class;
    return $self;
}

sub fetch {
    my ($self, $key) = @_;
    return undef;
}

sub store {
    my ($self, $key, $value) = @_;
    return -1
}

sub remove {
    my ($self, $key) = @_;
    return -1;
}

sub exist {
    my ($self, $key) = @_;
    return 0;
}

sub count {
    my ($self, $key) = @_;
    return 0;
}

sub index {
    my ($self, $key) = @_;
    return undef;
}

1;
