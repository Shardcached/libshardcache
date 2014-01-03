package Shardcache::Client;

use strict;
use IO::Socket::INET;
use Digest::SipHash qw/siphash64/;
use Algorithm::ConsistentHash::CHash;

our $VERSION = "0.01";

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
        $self->{_port} = [];
        foreach my $h (@$host) {
            if ($h !~ /[a-zA-Z0-9_\.]+:[a-zA-Z0-9_\.]+(:[0-9]+)?/) {
                die "Invalid host string $h";
            }
            my ($label, $addr, $port) = split(':', $h);
            push(@{$self->{_nodes}}, {
                    label => $label,
                    addr  => $addr,
                    port => $port });
        }
        $self->{_chash} = Algorithm::ConsistentHash::CHash->new(
                      ids      => [map { $_->{label} } @{$self->{_nodes}} ],
                      replicas => 200);
    } else {
        if ($host !~ /[a-zA-Z0-9_\.]+:[a-zA-Z0-9_\.]+(:[0-9]+)?/) {
            die "Invalid host string $host";
        }
        my ($addr, $port) = split(':', $host);
        push(@{$self->{_nodes}}, {
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
    my ($self, $hdr, $key, $value, $expire, $sock) = @_;

    my $templ = "C";
    my @vars = ($hdr);

    if ($key) {
        my $kbuf = _chunkize_var($key);
        $templ .= sprintf "a%dCC", length($kbuf);
        push @vars, $kbuf, 0x00, 0x00;
    } else {
        $templ .= "CC";
        push @vars, 0x00, 0x00;
    }

    if ($value) {
        $templ .= "C";
        push @vars, 0x80;

        my $vbuf = _chunkize_var($value);

        $templ .= sprintf "a%dCC", length($vbuf);
        push @vars, $vbuf, 0x00, 0x00;
        if ($expire) {
            $templ .= "CCCNCC";
            push @vars, 0x80, 0x00, 0x04, $expire, 0x00, 0x00;
        }
    }

    $templ .= "C";
    push @vars, 0x00;

    my $msg = pack $templ, @vars;

    if ($self->{_secret}) {
        my $sig = siphash64($msg,  pack("a16", $self->{_secret}));
        $msg .= pack("Q", $sig);
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
            $self->{_sock}->{"$addr:$port"}->write(pack("C4", 0x90, 0x00, 0x00, 0x00)) != 4)
        {
            $self->{_sock}->{"$addr:$port"} = IO::Socket::INET->new(PeerAddr => $addr,
                                                 PeerPort => $port,
                                                 Proto    => 'tcp');
        }

        $sock = $self->{_sock}->{"$addr:$port"};
    }
                                     
    my $wb = $sock->write($msg);
    
    # read the response
    my $in;
    my $data;


    # read the header
    if (read($sock, $data, 1) != 1) {
        delete $self->{_sock}->{"$addr:$port"};
        return undef;
    }

    $in .= $data;

    my $stop = 0;
    my $out; 

    # read the records
    while (!$stop) {
        if (read($sock, $data, 2) != 2) {
            return undef;
        }
        my ($len) = unpack("n", $data);
        $in .= $data;
        while ($len) {
            my $rb = read($sock, $data, $len);
            if ($rb <= 0) {
                return undef;
            }
            $in .= $data;
            $out .= $data;
            $len -= $rb;
            if ($len == 0) {
                if (read($sock, $data, 2) != 2) {
                    return undef;
                }
                ($len) = unpack("n", $data);
                $in .= $data;
            }
        }
        
        if (read($sock, $data, 1) != 1) {
            return undef;
        }
        $in .= $data;
        my ($sep) = unpack("C", $data);
        $stop = 1 if ($sep == 0);
        # TODO - should check if it's a correct rsep (0x80)
    }

    if ($self->{_secret}) {
        # now that we have the whole message, let's compute the signature
        # (we know it's 8 bytes long and is the trailer of the message
        my $signature = siphash64(substr($in, 0, length($in)),  pack("a16", $self->{_secret}));

        my $csig = pack("Q", $signature);

        my $rb = read($sock, $data, 8);
        if ($rb != 8) {
            return undef;
        }

        # $chunk now points at the signature
        if ($csig ne $data) {
            return undef;
        }
    }

    return $out;
}

sub get {
    my ($self, $key) = @_;
    return unless $key;
    return $self->send_msg(0x01, $key);
}

sub set {
    my ($self, $key, $value, $expire) = @_;
    return unless $key && defined $value;
    my $resp = $self->send_msg(0x02, $key, $value, $expire);
    return (defined $resp && $resp eq "OK")
}

sub del {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x03, $key);
    return ($resp eq "OK")
}

sub evi {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x04, $key);
    return ($resp eq "OK")
}

sub mgb {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x04, $key);
    return ($resp eq "OK")
}

sub mga {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x04, $key);
    return ($resp eq "OK")
}

sub _get_sock_for_peer {
    my ($self, $peer) = @_;
    my $addr;
    my $port;

    if (@{$self->{_nodes}} == 1) {
            $addr = $self->{_nodes}->[0]->{addr};
            $port = $self->{_nodes}->[0]->{port};
    } else {
        my ($node) = grep { $_->{label} eq $peer } @{$self->{_nodes}};
        if ($node) {
            $addr = $node->{addr};
            $port = $node->{port};
        } else {
            return undef;
        }
    }
    if (!$self->{_sock}->{"$addr:$port"} || !$self->{_sock}->{"$addr:$port"}->connected) {
        $self->{_sock}->{"$addr:$port"} = IO::Socket::INET->new(PeerAddr => $addr,
                                             PeerPort => $port,
                                             Proto    => 'tcp');
    }
    return $self->{_sock}->{"$addr:$port"};
}

sub chk {
    my ($self, $peer) = @_;
    my $sock = $self->_get_sock_for_peer($peer);

    return unless $sock;

    my $resp = $self->send_msg(0x31, undef, undef, undef, $sock);
    return ($resp eq "OK");
}

sub sts {
    my ($self, $peer) = @_;

    my $sock = $self->_get_sock_for_peer($peer);

    return unless $sock;

    my $resp = $self->send_msg(0x32, undef, undef, undef, $sock);
    return $resp;
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
