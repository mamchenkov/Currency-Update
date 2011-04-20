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
use Class::CSV;

use constant 'BASE_CURRENCY' => 'EUR';
use constant 'CURRENCY_FORMAT' => '%.4f';

use constant 'LEVEL_INFO' => 'info';
use constant 'LEVEL_DEBUG' => 'debug';
use constant 'LEVEL_DIE' => 'die';
use constant 'LEVEL_ERROR' => 'error';

my @eols = {
	'win' => "\r\n",
	'unix' => "\n",
};

# Making lock
unless (flock(DATA, LOCK_EX|LOCK_NB)) {
    print "$0 is already running. Exiting.\n";
	exit(1);
}

my $config = shift || die("No configuration given");
my %config = Config::General->new($config)->getall();

# Where from should we take the currency quotes
my $url = $config{'Links'}{'HellenicBank'};
my $boc_url = $config{'Links'}{'BankOfCyprus'};

# Currencies for which we always must have rates
my @required_currencies = split(/,/, $config{'Currencies'}{'Required'});
my $max_diff = $config{'Currencies'}{'RateThreshold'}; # in %


#  MAIN 
#

debug("Updating currencies", LEVEL_INFO);
debug("Getting currency rates from Hellenic Bank web site ...", LEVEL_INFO);
my $rates = get_hb_rates($url,\@required_currencies);
debug("Rates from HB: " . Dumper($rates), LEVEL_DEBUG);

debug("The HB currency rates are empty!", LEVEL_DIE) unless @$rates;

debug("Getting currency rates from BOC for cross-checking ...", LEVEL_INFO);
my $boc_rates = get_hb_rates($boc_url, \@required_currencies, [0, 0], 4, 1);
debug('BOC rates: ' . Dumper($boc_rates), LEVEL_DEBUG);

debug("The BOC currency rates are empty!", LEVEL_DIE) unless @$boc_rates;

debug("Verify rates ...", LEVEL_INFO);
my $ret = verify_rates($rates, $boc_rates);

debug("Calculating missing rates ...", LEVEL_INFO);
$rates = calculate_missing_rates($rates,\@required_currencies);
debug("HB + missing rates: " . Dumper($rates), LEVEL_DEBUG);

debug("Rotating rates ...", LEVEL_INFO);
$rates = rotate_rates($rates);
debug("All (rotated) rates: " . Dumper($rates), LEVEL_DEBUG);

debug("Saving rates in DB ...", LEVEL_INFO);
save_rates($rates);

build_report($rates);

debug("Finishing!", LEVEL_INFO);

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
		
		debug("Calculating rate: $from --> $to", LEVEL_DEBUG);

		# Find the two rates that are needed for conversion
		my $from_rate = 0;
		my $to_rate = 0;

		# TODO : to avoid repeated calculation we should go over results, not rates here
		foreach my $rate (@{ $rates }) {
			debug("Checking rate: " . Dumper($rate), LEVEL_DEBUG);
			if ($rate->{'from'} eq $from) {
				$from_rate = $rate->{'rate'};
			}
			elsif ($rate->{'from'} eq $to) {
				$to_rate = sprintf(CURRENCY_FORMAT, 1 / $rate->{'rate'});
			}
			debug("Got: from_rate=$from_rate; to_rate=$to_rate", LEVEL_DEBUG);
		}

		if ($from_rate != 0 && $to_rate !=0 && $to ne BASE_CURRENCY) {
			my %result = ();
			$result{'from'} = $from;
			$result{'to'} = $to;
			$result{'rate'} = sprintf(CURRENCY_FORMAT, $from_rate * $to_rate);
			$result{'comment'} = "Automatically calculated (from=$from_rate,to=$to_rate)";
			
			debug("RESULT: $from --> $to: $result{'rate'}", LEVEL_DEBUG);

			push @results, \%result;
		}
		else {
			debug("Something went wrong while calculating $from => $to!!!", LEVEL_ERROR);
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
		my $str = "cur_from = '" . $result->{'from'} . "', cur_to = '" . $result->{'to'} . "', rate = " . $result->{'rate'} ." , comments = '" . $result->{'comment'} . "'";
		print "$str\n";
	}
}

sub verify_rates {
	my($hb_rates, $boc_rates) = @_;

	for my $rate(@$hb_rates) {
		debug("Verify rate: " . Dumper($rate), LEVEL_DEBUG);
	
		for my $boc_rate (@$boc_rates) {
			if($boc_rate->{from} eq $rate->{from} && $boc_rate->{to} eq $rate->{to}) {
				debug("Found similar rate: $rate->{from} --> $rate->{to} ...", LEVEL_DEBUG);
				
				my $selling_diff = sprintf('%.2f', (abs($rate->{sell} - $boc_rate->{sell}) * 100) / $rate->{sell});
				my $buying_diff = sprintf('%.2f', (abs($rate->{buy} - $boc_rate->{buy}) * 100) / $rate->{buy});

				debug("Selling: HB=$rate->{sell}; BOC=$boc_rate->{sell} --> $selling_diff; buying: HB=$rate->{buy}; BOC=$boc_rate->{buy} --> $buying_diff", LEVEL_DEBUG);
				
				if($selling_diff > $max_diff or $buying_diff > $max_diff) {
					debug("The currency rate difference between HB and BOC - $selling_diff / $buying_diff, is greater then $max_diff! Currency from: $rate->{from}; currency to: $rate->{to}", LEVEL_DIE);
				}
			}
		}
	}
}

sub build_report {
	my($data) = @_;

	my $csv = Class::CSV->new(
		'fields' => [qw/from to rate comment/],
		'line_separator' => "\r\n",
	);

	for my $row (@$data) {
		$csv->add_line([$row->{from}, $row->{to}, $row->{rate}, $row->{comment}]);
	}

	open(my $fh, '>', 'test.csv');
	print $fh $csv->string();
	close($fh);

}

sub debug {
	my ($message, $level) = @_;
	print "$level $message\n";

	if ($level eq LEVEL_DIE) {
		die(1);
	}
}

__DATA__
