package Shardcache::Client;

use strict;
use IO::Socket::INET;
use Digest::SipHash qw/siphash64/;

our $VERSION = "0.01";

sub new {
    my ($class, $host, $secret) = @_;

    croak("The host parameter is mandatory!")
        unless($host);

    my ($addr, $port) = split(':', $host);

    $secret = 'default' unless($secret);

    my $self = { _addr => $addr,
                 _port => $port,
                 _secret => $secret,
               };

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
    my ($self, $hdr, $key, $value) = @_;

    my $templ = "C";
    my @vars = ($hdr);

    my $kbuf = _chunkize_var($key);
    $templ .= sprintf "a%dCC", length($kbuf);
    push @vars, $kbuf, 0x00, 0x00;

    if ($hdr == 0x02 && $value) {
        $templ .= "C";
        push @vars, 0x80;

        my $vbuf = _chunkize_var($value);

        $templ .= sprintf "a%dCC", length($vbuf);
        push @vars, $vbuf, 0x00, 0x00;
    }

    $templ .= "C";
    push @vars, 0x00;

    my $msg = pack $templ, @vars;

    my $sig = siphash64($msg,  pack("a16", $self->{_secret}));
    $msg .= pack("Q", $sig);

    my $sock = IO::Socket::INET->new(PeerAddr => $self->{_addr},
                                     PeerPort => $self->{_port},
                                     Proto    => 'tcp');
                                     
    print $sock $msg;
    
    # read the response
    my $in;
    my $data;
    while (read($sock, $data, 1024) > 0) {
        $in .= $data;
    }

    # now that we have the whole message, let's compute the signature
    # (we know it's 8 bytes long and is the trailer of the message
    my $signature = siphash64(substr($in, 0, length($in)-8),  pack("a16", $self->{_secret}));

    my $csig = pack("Q", $signature);

    my ($rhdr, $chunk) = unpack("Ca*", $in);

    my $out; 
    for (;;) {
        my $len;
        do {
            ($len, $chunk) = unpack("na*", $chunk);
            $out .= $len ? substr($chunk, 0, $len, "") : "";
        } while ($len > 0);
        my $char = unpack("C", substr($chunk,0, 1, ""));
        if ($char == 0x80) {
            next;
        } elsif($char == 0x00) {
            last;
        } else {
            # BAD FORMAT
            return;
        }
    }

    # $chunk now points at the signature
    if ($csig ne $chunk) {
        return undef;
    }

    return $out;
}

sub get {
    my ($self, $key) = @_;
    return unless $key;
    return $self->send_msg(0x01, $key);
}

sub set {
    my ($self, $key, $value) = @_;
    return unless $key && defined $value;
    my $resp = $self->send_msg(0x02, $key, $value);
    return (defined $resp && $resp eq "OK")
}

sub del {
    my ($self, $key) = @_;
    return unless $key;
    my $resp = $self->send_msg(0x03, $key);
    return ($resp eq "OK")
}

1;
__END__

=head1 NAME

Shardcache::Client - Client library to access shardcache nodes


=head1 SYNOPSIS

    use Shardcache::Client;

    $c = Shardcache::Client->new("http://localhost:4444", "my_shardcache_secret");

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
