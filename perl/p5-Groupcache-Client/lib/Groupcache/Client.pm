package Groupcache::Client;

use strict;
use IO::Socket::INET;

our $VERSION = "0.01";

sub new {
    my ($class, $host) = @_;

    my ($addr, $port) = split(':', $host);
    my $self = { _addr => $addr, _port => $port };
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
        my $vbuf = _chunkize_var($value);
        $templ .= sprintf "a%dCC", length($vbuf);
        push @vars, $vbuf, 0x00, 0x00;
    }

    my $msg = pack $templ, @vars;

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
    my ($rhdr, $len, $chunk) = unpack("Cna*", $in);

    return undef if ($len == 0);

    my $out = substr($chunk, 0, $len, "");
    while ($len == 65535) {
        ($len, $chunk) = unpack("na*", $chunk);
        $out .= substr($chunk, 0, $len, "");
    }
    return $out;
}

sub get {
    my ($self, $key) = @_;
    return $self->send_msg(0x01, $key);
}

sub set {
    my ($self, $key, $value) = @_;
    my $resp = $self->send_msg(0x02, $key, $value);
    return ($resp eq "OK")
}

sub del {
    my ($self, $key) = @_;
    my $resp = $self->send_msg(0x03, $key);
    return ($resp eq "OK")
}

1;
__END__

=head1 NAME

Groupcache::Client - Client library to access groupcache nodes


=head1 SYNOPSIS

    use Groupcache::Client;

    $c = Groupcache::Client->new("http://localhost:4444");

    $c->set("key", "value");

    $v = $c->get("key");

    $c->del("key");

=head1 DESCRIPTION

Client library to access groupcache nodes (based on libgroupcache)
This module allow committing get/set/del operations to the groupcache cloud
communicating with any of the nodes over their internal channel

=head1 SEE ALSO

=for :list
 * L<Groupcache>
 * L<Groupcache::Storage>
 * L<Groupcache::Storage::Mem>

=head1 AUTHOR

Andrea Guzzo, E<lt>xant@apple.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Andrea Guzzo

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
