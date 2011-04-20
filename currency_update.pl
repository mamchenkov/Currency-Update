#!/usr/bin/perl -w

#
# This script fetches currency exchange rates from the web site
# of Hellenic Bank.  It than makes sure that all the required
# rates exist and then saves everything into the database.
#
# Ideally, this script should work as a schedule job.
#

use strict;
use warnings;
use utf8;

use WWW::Mechanize;
use HTML::TableExtract;
use Config::General;
use Encode;
use Fcntl qw(:flock);
use POSIX qw(strftime);
use Data::Dumper;

use lib "bin/perl_utils";
use DBConnect;
use Common;

use constant 'BASE_CURRENCY' => 'EUR';
use constant 'CURRENCY_FORMAT' => '%.4f';

# Making lock
unless (flock(DATA, LOCK_EX|LOCK_NB)) {
    print "$0 is already running. Exiting.\n";
	exit(1);
}

# Where from should we take the currency quotes
my $url = 'https://www.hellenicbank.com/easyconsole.cfm/page/currencies'; #'http://www.hellenicbank.com/HB/content/en/treasury.jsp?lang=en';
my $boc_url = 'http://www.bankofcyprus.com.cy/en-GB/Cyprus/Services--Rates/Exchange-Rates/'; # 'http://www.bankofcyprus.com/Main/ExchangeRates.aspx?lang=en';

my $env = shift || 'local_dev';

my($bin_path) = $0 =~ m#(.*bin)#;
my %config = Config::General->new("$bin_path/perl_utils/util_conf.rc")->getall();

# DB config
my $db_data = $config{$env}{'db'};
my $db_table = 'currency_rates';
my $sugar_dbh = new DBConnect($db_data->{sugar_db});

my $log_level = 'DEBUG';
my $log_file = '/var/log/fxpro/currency_hb.log';
my $logger = create_logger($log_level, $log_file);
$Common::logger = $logger;

# Currencies for which we always must have rates
my @required_currencies = ('USD', 'EUR', 'CHF', 'JPY', 'GBP', 'RUB');
my $max_diff = 5; # in %

my $from = 'sugar@fxpro.com';
my $to = ['michael@fxpro.com'];
my $subject = ($env eq 'prod' ? '' : '[DEV] ') . 'Currency rates update';
$Common::smtp_host = 'it.fxpro.com';

#
#  MAIN 
#

$logger->info("Updating currencies");
$logger->info("Getting currency rates from Hellenic Bank web site ...");
my $rates = get_hb_rates($url,\@required_currencies);
$logger->debug("Rates from HB: " . Dumper($rates));

$logger->logdie("The HB currency rates are empty!") unless @$rates;

$logger->info("Getting currency rates from BOC for cross-checking ...");
my $boc_rates = get_hb_rates($boc_url, \@required_currencies, [0, 0], 4, 1);
$logger->debug('BOC rates: ' . Dumper($boc_rates));

$logger->logdie("The BOC currency rates are empty!") unless @$boc_rates;

$logger->info("Verify rates ...");
my $ret = verify_rates($rates, $boc_rates);

$logger->info("Calculating missing rates ...");
$rates = calculate_missing_rates($rates,\@required_currencies);
$logger->debug("HB + missing rates: " . Dumper($rates));

$logger->info("Rotating rates ...");
$rates = rotate_rates($rates);
$logger->debug("All (rotated) rates: " . Dumper($rates));

$logger->info("Saving rates in DB ...");
save_rates($rates);

build_report($rates);

$logger->info("Finishing!");

#
# Cleanup received strings
#
sub clean_value {
	my $value = shift;

	if ($value) {
		$value =~ s/\r//g;  # Windows line breaks...
		$value = join('', split(/\n/, $value)); # Join multiple lines into one
		$value =~ s/^\s+//; # Remove spaces in the beginning
		$value =~ s/\s+$//; # Remove spaces in the end
		chomp($value);
	}

	return $value;
}

#
# Get rates from Hellenic Bank
#
sub get_hb_rates {
	my $url = shift;
	my $reqs = shift;
	my $depth = shift || [0, 3]; # [3,10];
	my $selling_index = shift || 3;
	my $code_index = shift || 2;

	my @results = ();

	my $mech = WWW::Mechanize->new();
	$mech->get($url);

	if ($mech->success()) {

		my $te = HTML::TableExtract->new();
		my $content = $mech->content();
		if(!utf8::is_utf8($content)) {
			$content = decode_utf8($content);
		}	
		$te->parse($content);

		# Research with:  print $te->tables_report(1);
		my $table = $te->table(@$depth);
			
		foreach my $row ($table->rows) {
			my %result = ();
			
			print "ROW: " . Dumper($row);

			#
			# Hellenic columns: 1 - currency code, 2 - transfer selling, 3 - transfer buying
			# BOC columns: 1 - currency code, 4 - transfer selling, 3 - transfer buying
			#

			#$result{'name'} = clean_value(@$row[0]);
			$result{'from'} = uc(clean_value(@$row[$code_index])) || '';
			$result{'to'} = BASE_CURRENCY;
			$result{'sell'} = clean_value(@$row[$selling_index]) || '';
			$result{'buy'} = clean_value(@$row[4]) || '';
			$result{'comment'} = 'Hellenic Bank (sell=' . $result{'sell'} . ',buy=' . $result{'buy'} . ')';

			# Currency codes are always in 3-character strings
			if ($result{'from'} && (length($result{'from'}) == 3) && grep(/^$result{'from'}$/, @{ $reqs })) {
				$result{'rate'} = sprintf(CURRENCY_FORMAT, 1 / (($result{'sell'} + $result{'buy'}) / 2));
				push(@results, \%result);
			}
		}
	}
	else {
		die "Failed to fetch the URL : $url\n";
	}

	return \@results;
}

#
# We have all currency rates except for EUR.  This routine
# rotates known rates to get the EUR ones.
#
sub rotate_rates {
	my $rates = shift;
	my @results = ();

	foreach my $rate (@{ $rates }) {
		# Save the original rate first
		push @results, $rate;

		if ($rate->{'rate'} != 0 && $rate->{'to'} eq BASE_CURRENCY) {
			# Save the reversed rate first
			my %result = ();
			$result{'from'} = $rate->{'to'};
			$result{'to'} = $rate->{'from'};
			$result{'rate'} = sprintf(CURRENCY_FORMAT, (1 / $rate->{'rate'} ));
			$result{'comment'} = 'Reversed automatically (original=' . $rate->{'rate'} . ')';
			push @results, \%result;
		}
	}

	return \@results;
}

#
# If we are missing any rates from the bank, we should
# calculate the rates, so that all required currencies
# are covered.
#
sub calculate_missing_rates {
	my $rates = shift;
	my $reqs = shift;
	my @results = ();

	# Build full list of FROM=>TO pairs
	my @full_reqs_list = ();
	foreach my $from (@{ $reqs }) {
		foreach my $to (@{ $reqs }) {
			unless ($from eq $to || $from eq BASE_CURRENCY) {
				push @full_reqs_list, [$from, $to];
			}
		}
	}

	# Find out which pairs we don't have a rate for
	my @missing_pairs = ();
	foreach my $pair (@full_reqs_list) {
		my $from = @{ $pair }[0];
		my $to = @{ $pair}[1];

		my $found = 0;
		foreach my $rate (@{ $rates }) {
			if (($rate->{'from'} eq $from) && ($rate->{'to'} eq $to)) {
				$found++;
			}
		}
		unless ($found) {
			push @missing_pairs, [$from, $to];
		}
	}

	# Prepare for calculations
	foreach my $rate (@{ $rates }) {
		push @results, $rate;
	}

	# Calculate rate for the missing pairs
	foreach my $pair (@missing_pairs) {
		my $from = @{ $pair }[0];
		my $to = @{ $pair }[1];
		
		$logger->debug("Calculating rate: $from --> $to");

		# Find the two rates that are needed for conversion
		my $from_rate = 0;
		my $to_rate = 0;

		# TODO : to avoid repeated calculation we should go over results, not rates here
		foreach my $rate (@{ $rates }) {
			$logger->debug("Checking rate: " . Dumper($rate));
			if ($rate->{'from'} eq $from) {
				$from_rate = $rate->{'rate'};
			}
			elsif ($rate->{'from'} eq $to) {
				$to_rate = sprintf(CURRENCY_FORMAT, 1 / $rate->{'rate'});
			}
			$logger->debug("Got: from_rate=$from_rate; to_rate=$to_rate");
		}

		if ($from_rate != 0 && $to_rate !=0 && $to ne BASE_CURRENCY) {
			my %result = ();
			$result{'from'} = $from;
			$result{'to'} = $to;
			$result{'rate'} = sprintf(CURRENCY_FORMAT, $from_rate * $to_rate);
			$result{'comment'} = "Automatically calculated (from=$from_rate,to=$to_rate)";
			
			$logger->debug("RESULT: $from --> $to: $result{'rate'}");

			push @results, \%result;
		}
		else {
			$logger->error("Something went wrong while calculating $from => $to!!!");
		}
	}

	return \@results;
}

#
# Update the database
#
sub save_rates {
	my $rates = shift;

	foreach my $result (@{ $rates }) {
		my $sql = "INSERT INTO $db_table SET cur_from = '" . $result->{'from'} . "', cur_to = '" . $result->{'to'} . "', rate = " . $result->{'rate'} ." , comments = '" . $result->{'comment'} . "', created=NOW()";
		my $rv = $sugar_dbh->query($sql);
		unless ($rv) {
			$logger->error("Failed SQL query: $sql");
		}
	}
}

sub verify_rates {
	my($hb_rates, $boc_rates) = @_;

	for my $rate(@$hb_rates) {
		$logger->debug("Verify rate: " . Dumper($rate));
	
		for my $boc_rate (@$boc_rates) {
			if($boc_rate->{from} eq $rate->{from} && $boc_rate->{to} eq $rate->{to}) {
				$logger->debug("Found similar rate: $rate->{from} --> $rate->{to} ...");
				
				my $selling_diff = sprintf('%.2f', (abs($rate->{sell} - $boc_rate->{sell}) * 100) / $rate->{sell});
				my $buying_diff = sprintf('%.2f', (abs($rate->{buy} - $boc_rate->{buy}) * 100) / $rate->{buy});

				$logger->debug("Selling: HB=$rate->{sell}; BOC=$boc_rate->{sell} --> $selling_diff; buying: HB=$rate->{buy}; BOC=$boc_rate->{buy} --> $buying_diff");
				
				if($selling_diff > $max_diff or $buying_diff > $max_diff) {
					$logger->logdie("The currency rate difference between HB and BOC - $selling_diff / $buying_diff, is greater then $max_diff! Currency from: $rate->{from}; currency to: $rate->{to}");
				}
			}
		}
	}
}

sub build_report {
	my($data) = @_;

	my $body = '<table width="80%"><tr bgcolor="#C0C0C0"><th>#</th><th>From</th><th>To</th><th>Rate</th><th>Comment</th></tr>';
	my $count = 0;
	for my $row (@$data) {
		my $bgcolor = ++$count%2 ? '#F1F1F1' : '#FFFFFF';
		$body .= "<tr bgcolor='$bgcolor'><td>$count</td><td>$row->{from}</td><td>$row->{to}</td><td>$row->{rate}</td><td>$row->{comment}</td></tr>";
	}
	$body .= "</table>";

	my $ret = send_email($from, $to, $subject, $body, []);
}

__DATA__
