use 5.008005;
use ExtUtils::MakeMaker;
WriteMakefile(
    NAME           => 'Groupcache::Client',
    VERSION_FROM   => 'lib/Groupcache/Client.pm',
    PREREQ_PM      => {
      'Test::More'        => 0,
      'Groupcache'        => 0,
      'Net::Socket::INET' => 0,
    },
    ABSTRACT_FROM  => 'lib/Groupcache/Client.pm',
    AUTHOR         => [
        'Andrea Guzzo <xant@cpan.org>',
    ],
);

