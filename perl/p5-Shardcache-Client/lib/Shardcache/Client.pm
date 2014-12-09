package Shardcache::Client;

use strict;
use IO::Socket::INET;
use Digest::SipHash qw/siphash/;
use Algorithm::ConsistentHash::CHash;

our $VERSION = "0.05";

sub _read_bytes {
    my ($sock, $len) = @_;
    my $to_read = $len;
    my $read;
    my $data;
    while ($read != $len) {
        my $buffer;
        my $rb = read($sock, $buffer, $to_read); 
        last if ($rb <= 0);
        $data .= $buffer;
        $read += $rb;
        $to_read -= $rb;
    }

    return ($read == $len) ? $data : undef;
}

sub new {
    my ($class, $host, $secret) = @_;

    croak("The host parameter is mandatory!")
        unless($host);

    $secret = '' unless($secret);

    my $self = { 
                 _secret => $secret,
                 _nodes => [],
               };


    if (ref($host) && ref($host) eq "ARRAY") {
        foreach my $h (@$host) {
            if ($h !~ /^[a-zA-Z0-9_\.\-]+:[a-zA-Z0-9_\.\-]+(?:[:][0-9]+)?$/) {
                die "Invalid host string $h";
            }
            my ($label, $addr, $port) = split(':', $h);
            push(@{$self->{_nodes}}, {
                    label => $label,
                    addr  => $addr,
                    port => $port
                });
        }
        $self->{_chash} = Algorithm::ConsistentHash::CHash->new(
                      ids      => [map { $_->{label} } @{$self->{_nodes}} ],
                      replicas => 200);
    } else {
        if ($host !~ /^[a-zA-Z0-9_\.\-]+(?:[:][0-9]+)?$/) {
            die "Invalid host string $host";
        }
        my ($addr, $port) = split(':', $host);
        push(@{$self->{_nodes}}, {
                label => "$addr:$port",
                addr => $addr,
                port => $port
            });
    }

    bless $self, $class;
    
    return $self;
}

sub _chunkize_var {
    my ($var) = @_;
    my $templ;
    my @vars;
    my $vlen = length($var); 
    while ($vlen > 0) {
        if ($vlen <= 65535) {
            $templ .= "na$vlen";
            push(@vars, $vlen, $var);
            $vlen = 0;
        } else {
            $templ .= "na65535";
            my $substr = substr($var, 0, 65535, "");
            $vlen = length($var);
            push(@vars, 65535, $substr);
        }
    }
    return pack $templ, @vars;
}

sub send_msg {
    my ($self, $hdr, $records, $sock) = @_;

    my $templ = "C";
    my @vars = ($hdr);

    my $key;

    my $cnt = 0;
    $records = [ $records ] unless (ref($records) && ref($records) eq "ARRAY");
    foreach my $record (@$records) {
        if ($cnt++) {
            $templ .= "C";
            push @vars, 0x80;
        } else {
            $key = $record;
        }

        if ($record) {
            my $buf = _chunkize_var($record);
            $templ .= sprintf "a%dCC", length($buf);
            push @vars, $buf, 0x00, 0x00;
        } else {
            $templ .= "CC";
            push(@vars, 0x00, 0x00);
        }
    }

    if (!$cnt) {
        $templ .= "CC";
        push @vars, 0x00, 0x00;
    }

    $templ .= "C";
    push @vars, 0x00;

    my $msg = pack $templ, @vars;

    if ($self->{_secret}) {
        my ($hi, $lo) = siphash($msg, pack("a16", $self->{_secret}));
        $msg .= pack("LL", $hi, $lo);
    }

    my $addr;
    my $port;

    if (!$sock) {
        if (@{$self->{_nodes}} == 1) {
            $addr = $self->{_nodes}->[0]->{addr};
            $port = $self->{_nodes}->[0]->{port};
        } else {
            my ($node) = grep { $_->{label} eq $self->{_chash}->lookup($key) } @{$self->{_nodes}};
            if ($node) {
                $addr = $node->{addr};
                $port = $node->{port};
            } else {
                my $index = rand() % @{$self->{_nodes}};
                $addr = $self->{_nodes}->[$index]->{addr};
                $port = $self->{_nodes}->[$index]->{port};
            }

        }
        
        if (!$self->{_sock}->{"$addr:$port"} || !$self->{_sock}->{"$addr:$port"}->connected ||
            $self->{_sock}->{"$addr:$port"}->write(pack("C1", 0x90)) != 1)
        {
            $self->{_sock}->{"$addr:$port"} = IO::Socket::INET->new(PeerAddr => $addr,
                                                                    PeerPort => $port,
                                                                    Proto    => 'tcp');
        }

        $sock = $self->{_sock}->{"$addr:$port"};
    }
                                     
    return unless $sock->write(pack("C4", 0x73, 0x68, 0x63, 0x01));

    if ($self->{_secret}) {
        return undef unless $sock->write(pack("C", 0xF0));
    }

    my $wb = $sock->write($msg);
    
    # read the response
    my $in;

   # read the magic
    my $magic = _read_bytes($sock, 4);
    if (!$magic) {
        delete $self->{_sock}->{"$addr:$port"} if ($addr);
        return undef;
    }

    # read the signature if necessary
    if ($self->{_secret}) {
        my $byte = _read_bytes($sock, 1);
        if (!$byte) {
            delete $self->{_sock}->{"$addr:$port"} if ($addr);
            return undef;
        }

        if (unpack("C", $byte) != 0xF0) {
            return undef;
        }
    }
 
    # read the header
    my $data = _read_bytes($sock, 1);
    if (!$data) {
        delete $self->{_sock}->{"$addr:$port"} if ($addr);
        return undef;
    }

    if (unpack("C", $data) == 0xF0) {
        delete $self->{_sock}->{"$addr:$port"} if ($addr);
        return undef;
    }

    $in .= $data;

    my $stop = 0;
    my $out; 

    # read the records
    my @output_records;
    while (!$stop) {
        unless ($data = _read_bytes($sock, 2)) {
            delete $self->{_sock}->{"$addr:$port"} if ($addr);
            return undef;
        }
        my ($len) = unpack("n", $data);
        while ($len) {
            $in .= $data;
            unless ($data = _read_bytes($sock, $len)) {
                delete $self->{_sock}->{"$addr:$port"} if ($addr);
                return undef;
            }
            $in .= $data;
            $out .= $data;
            $len = 0;
            unless ($data = _read_bytes($sock, 2)) {
                delete $self->{_sock}->{"$addr:$port"} if ($addr);
                return undef;
            }
            ($len) = unpack("n", $data);
            $in .= $data;
        }
        
        unless ($data = _read_bytes($sock, 1)) {
            delete $self->{_sock}->{"$addr:$port"} if ($addr);
            return undef;
        }
        $in .= $data;
        my ($sep) = unpack("C", $data);
        # TODO - should check if it's a correct rsep (0x80)
        $stop = 1 if ($sep == 0);
        push(@output_records, $out);
        undef($out);
    }

    if ($self->{_secret}) {
        # now that we have the whole message, let's compute the signature
        # (we know it's 8 bytes long and is the trailer of the message
        my ($hi, $lo) = siphash(substr($in, 0, length($in)), pack("a16", $self->{_secret}));
        my $csig = pack("LL", $hi, $lo);

        unless ($data = _read_bytes($sock, 8)) {
            delete $self->{_sock}->{"$addr:$port"} if ($addr);
            return undef;
        }

        if ($csig ne $data) {
            delete $self->{_sock}->{"$addr:$port"} if ($addr);
            return undef;
        }
    }

    return wantarray ? @output_records : $output_records[0];
}

sub get {
    my ($self, $key) = @_;
    return unless $key;
    return $self->send_msg(0x01, $key);
}

sub offset {
    my ($self, $key, $offset, $len) = @_;
    return unless $key;
    return $self->send_msg(0x06, [$key, pack("N", $offset), pack("N", $len)]);
}

sub set {
    my ($self, $key, $value, $expire) = @_;
    return unless $key && defined $value;
    my @records = ($key, $value);

    push(@records, pack("N", $expire))
        if ($expire);

    my $resp = $self->send_msg(0x02, \@records);
    return (defined $resp && unpack("C", $resp) == 0x00);
}

sub add {
    my ($self, $key, $value, $expire) = @_;
    return unless $key && defined $value;
    my @records = ($key, $value);

    push(@records, pack("N", $expire))
        if ($expire);

    my $resp = $self->send_msg(0x07, \@records);
    return (defined $resp && unpack("C", $resp) == 0x00);
}

sub del {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x03, $key);
    return (unpack("C", $resp) == 0x00);
}

sub evi {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x04, $key);
    return (unpack("C", $resp) == 0x00);
}

sub evict {
    my $self = shift;
    return $self->evi(@_);
}

sub mgb {
    my ($self, $key) = @_;
    return unless $key;
    # TODO - nodes-list must be sent together with the MGB command
    my $resp = $self->send_msg(0x22, $key);
    return (unpack("C", $resp) == 0x00);
}

sub migration_begin {
    my $self = shift;
    return $self->mgb(@_);
}

sub mge {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x23, $key);
    return (unpack("C", $resp) == 0x00);
}

sub migration_end {
    my $self = shift;
    return $self->mge(@_);
}

sub mga {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x21, $key);
    return (unpack("C", $resp) == 0x00);
}

sub migration_abort {
    my $self = shift;
    return $self->mga(@_);
}

sub _get_sock_key_for_node {
    my ($self, $node) = @_;
    my $addr;
    my $port;

    if (@{$self->{_nodes}} == 1) {
            $addr = $self->{_nodes}->[0]->{addr};
            $port = $self->{_nodes}->[0]->{port};
    } else {
        my ($node) = grep { $_->{label} eq $node } @{$self->{_nodes}};
        if ($node) {
            $addr = $node->{addr};
            $port = $node->{port};
        } else {
            return undef;
        }
    }
    return "$addr:$port";
}

sub _get_sock_for_node {
    my ($self, $node) = @_;

    my $sock_key = $self->_get_sock_key_for_node($node);

    if (!$self->{_sock}->{$sock_key} || !$self->{_sock}->{$sock_key}->connected ||
        $self->{_sock}->{$sock_key}->write(pack("C1", 0x90)) != 1)
    {
        my ($addr, $port) = split(':', $sock_key);
        $self->{_sock}->{$sock_key} = IO::Socket::INET->new(
                                             PeerAddr => $addr,
                                             PeerPort => $port,
                                             Proto    => 'tcp');
    }

    return $self->{_sock}->{$sock_key};
}

sub _delete_sock_for_node {
    my ($self, $node) = @_;

    my $sock_key = $self->_get_sock_key_for_node($node);

    delete $self->{_sock}->{$sock_key};
}

sub index {
    my ($self, $node) = @_;

    my %index;

    my @nodes = $node
              ? ($node)
              : map { $_->{label} } @{$self->{_nodes}};

    foreach my $n (@nodes) {
        my $sock = $self->_get_sock_for_node($n);

        return unless $sock;


        my $resp = $self->send_msg(0x41, undef, $sock);
        while (length($resp)) {
            my $ksize = unpack("N", $resp);
            $resp = substr($resp, 4);
            my $kdata = unpack("a$ksize", $resp);
            $resp = substr($resp, $ksize);
            my $vlen = unpack("N", $resp); 
            $resp = substr($resp, 4);
            $index{$kdata} = $vlen
                if ($ksize && $kdata);
        }
    }
    return wantarray ? %index : \%index;
}

sub check {
    my ($self, $node) = @_;
    my $sock = $self->_get_sock_for_node($node);

    return unless $sock;

    my $resp = $self->send_msg(0x31, undef, $sock);
    $self->_delete_sock_for_node($node) unless $resp;

    return $resp ? (unpack("C", $resp) == 0x00) : undef;
}

sub chk {
    my $self = shift;
    # XXX - deprecated
    return $self->check(@_);
}

sub status {
    my ($self, $node) = @_;

    my $sock = $self->_get_sock_for_node($node);

    return unless $sock;

    my $resp = $self->send_msg(0x32, undef, $sock);
    $self->_delete_sock_for_node($node) unless $resp;

    return $resp;
}

sub sts {
    my $self = shift;
    # XXX - deprecated
    return $self->status(@_);
}

1;
__END__

=head1 NAME

Shardcache::Client - Client library to access shardcache nodes


=head1 SYNOPSIS

    use Shardcache::Client;

    # To connect to one of the nodes and perform any operation
    $c = Shardcache::Client->new("localhost:4444", "my_shardcache_secret");

    # If you want Shardcache::Client to make sure requets go to the owner of the key
    $c = Shardcache::Client->new(["peer1:localhost:4443", "peer2:localhost:4444", ...],
                                 "my_shardcache_secret");

    $c->set("key", "value");

    $v = $c->get("key");

    $c->del("key");

=head1 DESCRIPTION

Client library to access shardcache nodes (based on libshardcache)
This module allow committing get/set/del operations to the shardcache cloud
communicating with any of the nodes over their internal channel

=head1 SEE ALSO

=for :list
 * L<Shardcache>
 * L<Shardcache::Client::Fast>
 * L<Shardcache::Storage>
 * L<Shardcache::Storage::Mem>

=head1 AUTHOR

Andrea Guzzo, E<lt>xant@apple.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Andrea Guzzo

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
