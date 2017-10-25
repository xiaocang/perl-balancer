#!/usr/bin/env perl
package Chash;

use strict;
use warnings;
use String::CRC32;
use Smart::Comments;
use Exporter;
use Clib;

use constant {CONSISTENT_POINTS => 160};

# local
sub _precompute($);
sub _find_id($$$);

# export
sub new($$);
sub reinit($$);
sub set($$$);
sub find($$);
sub next($$);

# local obj
sub _delete($$);
sub _incr($$$);
sub _desc($$$);

sub ffi_new {
    my ($t, $n) = @_;
    return [ map {$t} (0..$n) ];
}

sub _precompute {
    my $nodes = shift;

    my ($n, $total_weight) = (0, 0);
    while (my ($id, $weight) = each(%$nodes)) {
        $n++;
        $total_weight += $weight;
    }

    my $newnodes = $nodes;

    my $ids     = {};
    my $npoints = $total_weight * CONSISTENT_POINTS;
    my $points  = ffi_new($chash_point_t, $npoints);

    my ($start, $index) = (0, 0);
    while (my ($id, $weight) = each(%$nodes)) {
        my $num = $weight * CONSISTENT_POINTS;
        my $base_hash = crc32($id) ^ 0xffffffff;

        $index ++;
        $ids{$index} = $id;

        chash_point_init($points, $base_hash, $start, $num, $index);

        $start += $num;
    }

    chash_point_sort($points, $npoints);

    return $ids, $points, $npoints, $newnodes;
}

sub new {
    my $class = shift;
    my $nodes = shift;

    my ($ids, $points, $npoints, $newnodes) = _precompute($nodes);

    return bless {
        nodes   => $newnodes,
        ids     => $ids,
        points  => $points,
        npoints => $npoints,
        size    => $npoints,
    }, $class;
}

1;
