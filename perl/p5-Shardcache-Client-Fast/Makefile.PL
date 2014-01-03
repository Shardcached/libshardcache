use 5.010000;
use ExtUtils::MakeMaker;

my $INC = "-I. ";
my $LIBS = "-lshardcache ";
my $DEFINE = ""; 

my $SHC_INCDIR;
my $SHC_LIBDIR;
if (-f "/usr/include/shardcache.h") {
    $SHC_INCDIR = "/usr/include";
    $SHC_LIBDIR = "/usr/lib";
} elsif (-f "/usr/local/include/shardcache.h") {
    $SHC_INCDIR = "/usr/local/include";
    $SHC_LIBDIR = "/usr/local/lib";
} elsif (-f "/opt/local/include/shardcache.h") {
    $SHC_INCDIR = "/opt/local/include";
    $SHC_LIBDIR = "/opt/local/lib";
} else {
    die "No libshardcache installed"
}

$INC .= "-I$SHC_INCDIR"
    if ($SHC_INCDIR);
$LIBS .= "-L$SHC_LIBDIR"
    if ($SHC_LIBDIR);

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Shardcache::Client::Fast',
    VERSION_FROM      => 'lib/Shardcache/Client/Fast.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Shardcache/Client/Fast.pm', # retrieve abstract from module
       AUTHOR         => 'xant <xant@macports.org>') : ()),
    LIBS              => [$LIBS], # e.g., '-lm'
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
                                     NAME         => 'Shardcache::Client::Fast',
                                     NAMES        => \@names,
                                     DEFAULT_TYPE => 'IV',
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
