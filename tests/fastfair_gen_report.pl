#!/usr/bin/perl
use v5.10;
use utf8;
use autodie;
use Excel::Writer::XLSX;

my $workbook = Excel::Writer::XLSX->new( 'fastfair-report.xlsx' );
my $worksheet = $workbook->add_worksheet();
$workbook->add_format(xf_index => 0, font => 'Courier New', align => 'right');

my $row = 0;
my $col = 0;
sub writesheet {
    my $key = shift;
    my $value = shift;
    $col == 1 && $worksheet->write($row, 0, $key);
    $worksheet->write($row, $col, $value);
    $row++;
}

chomp(@lines = <>);

my $num = '\s*(\d+(?:\.\d+)?)';
foreach (@lines) {
    if (/fastfair, workload(\d+),/) {
        $col++;
        $row = 0;
        writesheet "Thread", $1;
    } elsif (/Load_Throughput: load, ${num} ,/) {
        writesheet "load iops", $1/1000;
    } elsif (/Put_Throughput: run, ${num} ,/) {
        writesheet "put iops", $1/1000;
    } elsif (/Get_Throughput: run, ${num} ,/) {
        writesheet "get iops", $1/1000;
    } elsif (/Scan_(\d+)_Throughput: run, ${num} ,/) {
        writesheet "scan ${1} iops", $2/1000;
    }
}

$workbook->close();