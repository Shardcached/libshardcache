use 5.008005;
use ExtUtils::MakeMaker;

my $SHARDCACHE_INCDIR=$ENV{SHARDCACHE_INCDIR};
my $SHARDCACHE_LIBDIR=$ENV{SHARDCACHE_LIBDIR};

my $INC = "-I.";
$INC .= " -I$SHARDCACHE_INCDIR" if ($SHARDCACHE_LIBDIR);

my $LIB = "-lshardcache";
$LIB .= " -L$SHARDCACHE_LIBDIR" if ($SHARDCACHE_LIBDIR);

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Shardcache',
    VERSION_FROM      => 'lib/Shardcache.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Shardcache.pm', # retrieve abstract from module
       AUTHOR         => 'Andrea Guzzo <xant@xant.net>') : ()),
    LIBS              => $LIB, # e.g., '-lm'
    DEFINE            => '', # e.g., '-DHAVE_SOMETHING'
    INC               => $INC, # e.g., '-I. -I/usr/include/other'
	# Un-comment this if you add C files to link with later:
    # OBJECT            => '$(O_FILES)', # link all the C files too
);
if  (eval {require ExtUtils::Constant; 1}) {
  # If you edit these definitions to change the constants used by this module,
  # you will need to use the generated const-c.inc and const-xs.inc
  # files to replace their "fallback" counterparts before distributing your
  # changes.
  my @names = (qw());
  ExtUtils::Constant::WriteConstants(
                                     NAME         => 'Shardcache',
                                     NAMES        => \@names,
                                     DEFAULT_TYPE => 'PV',
                                     C_FILE       => 'const-c.inc',
                                     XS_FILE      => 'const-xs.inc',
                                  );

}
else {
  use File::Copy;
  use File::Spec;
  foreach my $file ('const-c.inc', 'const-xs.inc') {
    my $fallback = File::Spec->catfile('fallback', $file);
    copy ($fallback, $file) or die "Can't copy $fallback to $file: $!";
  }
}
