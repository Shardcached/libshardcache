use 5.008005;
use ExtUtils::MakeMaker;
WriteMakefile(
    NAME           => 'Shardcache::Client',
    VERSION_FROM   => 'lib/Shardcache/Client.pm',
    PREREQ_PM      => {
      'Test::More'       => 0,
      'IO::Socket::INET' => 0,
      'Digest::SipHash'  => 0,
      'Algorithm::ConsistentHash::CHash' => 0
    },
    ABSTRACT_FROM  => 'lib/Shardcache/Client.pm',
    AUTHOR         => 'Andrea Guzzo <xant@cpan.org>',
);

