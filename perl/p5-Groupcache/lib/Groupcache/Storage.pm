package Groupcache::Storage;

use strict;

sub new {
    my ($class) = @_;
    my $self = {};
    bless $self, $class;
    return $self;
}

sub fetch {
    my ($self, $key) = @_;
}

sub store {
    my ($self, $key, $value) = @_;
}

sub remove {
    my ($self, $key) = @_;
}

1;
