package POE::SAPI::DBIO;

use 5.010001;
use strict;
use warnings;

use POE;

our $VERSION = '0.02';

use FindBin qw($Bin);
use Data::Dumper;
use DBD::SQLite;

my $base = $Bin . '/../etc/';
my $var = $Bin . '/../var/';

my $config = 'config.db';
my $cache = 'cache.db';

my $defaultConfig;

my %initialized = (
	config	=>	0,
	cache	=>	0,
);

sub new {
        my $package = shift;
        my %opts    = %{$_[0]} if ($_[0]);
        $opts{ lc $_ } = delete $opts{$_} for keys %opts;       # convert opts to lower case
        my $self = bless \%opts, $package;

        $self->{start} = time;
        $self->{cycles} = 0;

        $self->{me} = POE::Session->create(
                object_states => [
                        $self => {
                                _start          =>      'initLauncher',
                                loop            =>      'keepAlive',
                                _stop           =>      'killLauncher',
				newDB		=>	'newDB',
				newDBbad	=>	'newDBbad',
				valDBok		=>	'valDBok',
				valDBbad	=>	'valDBbad',
				newDBok		=>	'newDBok',
				void		=>	'void',
				initConfig	=>	'initConfig',
				reqCacheTable	=>	'reqCacheTable',
				cacheCreateOK	=>	'cacheCreateOK',
				cacheCreateBAD	=>	'cacheCreateBAD',
				cacheCreateTable=>	'cacheCreateTable',
                        },
                        $self => [ qw (   ) ],
                ],
        );
}

sub keepAlive {
        my ($kernel,$session)   = @_[KERNEL,SESSION];
        my $self = shift;
        $kernel->delay('loop' => 1);
        $self->{cycles}++;
}
sub killLauncher { warn "Session halting"; }
sub initLauncher {
	my ($self,$kernel) = @_[OBJECT,KERNEL];
	$kernel->yield('loop'); 
	$kernel->alias_set('DBIO');
	$kernel->post($self->{parent},'register',{ name=>'DBIO', type=>'local' });
}
sub newDB {
        my ($kernel,$self,$defaults) = @_[KERNEL,OBJECT,ARG0];

	$defaultConfig = $defaults;

	$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Creating new config.db from defaults (../etc/defaults)", src=>"DBIO" });

        $self->{dbconfig} = POE::Component::EasyDBI->spawn(
                alias		=> 'dbconfig',
                dsn		=> 'dbi:SQLite:dbname='.$base.$config,
                username	=> '',
                password	=> '',
		connected	=> ['DBIO','newDBok'],
		connect_error	=> ['DBIO','newDBbad'],
                options		=> { AutoCommit => 1, },
        );
}
sub newDBok {
	my ($kernel,$self,$arg0) = @_[KERNEL,OBJECT,ARG0];

	if (!$arg0->{sql}) {
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"DB creation succeeded", class=>"normal" });
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Populating DB", class=>"normal" });
		$kernel->post(
			'dbconfig',
			do => {
				sql		=> 'CREATE TABLE config (field TEXT,value TEXT)',
				event		=> 'newDBok',
				primary_key	=> 1,
			}
		);
	} elsif ($arg0->{sql} =~ m#^CREATE TABLE config#) {
		foreach my $key (keys %{$defaultConfig}) {
			$kernel->post(
				'dbconfig',
				do => {
					sql		=> 'INSERT INTO config (field,value) VALUES(?,?)',
					placeholders	=> [$key,$defaultConfig->{$key}],
					event		=> 'void',
				}
			);
		}
		$kernel->post(
			'dbconfig',
			do => {
				sql             => 'INSERT INTO config (field,value) VALUES(?,?)',
				placeholders    => ["created",time],
				event           => 'void',
			}
		);
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"DB population success!", class=>"normal" });
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Resuming normal startup", class=>"normal" });
		$kernel->yield("initConfig","dbconfig");
	}
}
sub newDBbad {
        my ($kernel,$self,$defaults) = @_[KERNEL,OBJECT,ARG0];

	$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"DB creation failed (check permissions ../etc and if it exists)", src=>'DBIO', class=>'critical' });
}
sub initConfig {
	my ($kernel,$self,$db) = @_[KERNEL,OBJECT,ARG0];

	if ($db) { 
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Reusing pre existing DB connection", src=>'DBIO', class=>'performance' });
		$kernel->yield('valDBok');
	}
	else { 
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Attempting to connect to DB", src=>'DBIO', class=>'normal' });
		$self->{dbconfig} = POE::Component::EasyDBI->spawn(
			alias       => 'dbconfig',
			dsn         => 'dbi:SQLite:dbname='.$base.$config,
			username    => '',
			password    => '',
			connected       => ['DBIO','valDBok'],
			connect_error   => ['DBIO','valDBbad'],
			options     => { AutoCommit => 0 },
		);
	}

}
sub valDBok { 
	my ($kernel,$self,$db) = @_[KERNEL,OBJECT,ARG0];

	if (!$db->{sql}) { 
		$kernel->post(
			'dbconfig',
			hashhash => {
				sql             => 'SELECT * FROM config ',
				event           => 'valDBok',
			}
		);
	} else {
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"DB Connection established", src=>"DBIO", class=>"debug" });

		if (!$db->{result}->{admin}->{value}) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Failed admin verification", src=>'DBIO', class=>'normal' }); }
		elsif ((!$db->{result}->{created}->{value}) && ($db->{result}->{created}->{value} !~ m#^(\d+|auto)$#)) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Failed created verification" }); }
		elsif (!$db->{result}->{db}->{value}) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Failed DB verification" }); }
		elsif ((!$db->{result}->{port}->{value}) || ($db->{result}->{port}->{value} !~ m#^(\d+|auto)$#)) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Failed port verification" }); }
		elsif (!$db->{result}->{host}->{value}) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Failed port verification" }); }
		else { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"DB validation: OK" }); }

		$kernel->post($self->{parent},"boot", {
			type	=>	"initial",
			config	=>	{ 
				admin	=>	$db->{result}->{admin}->{value},
				created	=>	$db->{result}->{created}->{value},
				db	=>	$db->{result}->{db}->{value},
				port	=>	$db->{result}->{port}->{value},
				host	=>	$db->{result}->{host}->{value}
			}
		});

		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"DB 'config' marked ready." });
		$initialized{config} = 1;
	}
}
sub valDBbad { 
	my ($kernel,$self,$db) = @_[KERNEL,OBJECT,ARG0];

	$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"DB integrity failed connectivity check! (check permissions of config.db)", src=>"DBIO" });
	$kernel->post($self->{parent},"abort",{ type=>"abort", msg=>"DB initilization failed even though config.db was present, maybe its corrupt? hope you have a backup :-)" });
}
sub reqCacheTable {
	my ($kernel,$self,$req,$recall) = @_[KERNEL,OBJECT,ARG0,ARG1];

	if (!$recall) {
		if (($req->{type}) && ($req->{src})) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Request for table $req->{type} by $req->{src}", src=>"DBIO" }); }
		else { 
			if ((!$req->{type}) && (!$req->{src})) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Request for table DENIED no type or src passed!", src=>"DBIO" }); }
			if (!$req->{src}) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Request for table DENIED no src passed!", src=>"DBIO" }); }
			if (!$req->{type}) { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Request for table DENIED no type passed!", src=>"DBIO" }); }
			return ;
		}
	}

	if (!$initialized{cache}) { 
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Cache table initilizing.", src=>"DBIO", level=>"debug" });
		if ($self->{cacheWait}) { push @{$self->{cacheWait}},$req; }
		else { $self->{cacheWait} = [$req]; }
		cacheCreateDB($kernel,$self);
	} else {
		$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Cache table initilized processing table creation list.", src=>"DBIO", level=>"debug" });

		foreach my $oreq (@{$self->{cacheWait}}) {
			$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"src: $oreq->{src}", src=>"DBIO", level=>"debug" });;
			$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"fieldCount: ".scalar(@{$oreq->{fields}}), src=>"DBIO", level=>"debug" });

			my $tquery = "";
			foreach my $sql (@{$oreq->{fields}}) { $tquery = join(',',"$sql->{sql}",$tquery); }
			$tquery =~ s#^(.*),$#$1#;

			$kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"SQL: CREATE TABLE $oreq->{type} ($tquery)", src=>"DBIO", level=>"debug" });

			$kernel->post(
				'dbcache',
				do => {
					sql             => "CREATE TABLE $oreq->{type} ($tquery)",
					event           => $_[SESSION]->postback("cacheCreateTable",$oreq),
				} 
			);
		}
	}
}

sub cacheCreateOK {
	my ($kernel,$self,$req) = @_[KERNEL,OBJECT,ARG0];
	$initialized{cache} = 1;
	$kernel->yield('reqCacheTable',0,1);
}
sub cacheCreateBAD {
	my ($kernel,$self,$req) = @_[KERNEL,OBJECT,ARG0];
}

sub cacheCreateDB {
	my ($kernel,$self) = @_;

	if (!$self->{cachewait}) { $self->{cachewait} = 1; } else { $kernel->post($self->{parent},"passback",{ type=>"debug", msg=>"Duplicate create cache request, ignoring.", src=>"DBIO", level=>"debug" }); return; }

	$self->{dbcache} = POE::Component::EasyDBI->spawn(
		alias       => 'dbcache',
		dsn         => 'dbi:SQLite:dbname='.$var.$cache,
		username    => '',
		password    => '',
		connected       => ['DBIO','cacheCreateOK'],
		connect_error   => ['DBIO','cacheCreateBAD'],
		options     => { AutoCommit => 1 },
	);

}
sub cacheCreateTable {
	my ($kernel,$self,$req,$result) = @_[KERNEL,OBJECT,ARG0,ARG1];

	my ($sid,$handler) = @{$req->[0]{success}};
	my $name = $req->[0]{type};

	$kernel->post($sid,$handler,$name,\%{$req->[0]});
}
sub void { }

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

POE::SAPI::DBIO - Perl extension for blah blah blah

=head1 SYNOPSIS

  use POE::SAPI::DBIO;

=head1 DESCRIPTION

This is a CORE module of L<POE::SAPI> and should not be called directly.

=head2 EXPORT

None by default.

=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Paul G Webster, E<lt>paul@daemonrage.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Paul G Webster

All rights reserved.

Redistribution and use in source and binary forms are permitted
provided that the above copyright notice and this paragraph are
duplicated in all such forms and that any documentation,
advertising materials, and other materials related to such
distribution and use acknowledge that the software was developed
by the 'blank files'.  The name of the
University may not be used to endorse or promote products derived
from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.


=cut
