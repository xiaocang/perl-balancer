#!/usr/bin/env perl
package Balancer::Consistance;

use strict;
use warnings;
use String::CRC32;
use Smart::Comments;
use Exporter 'import';
use POSIX;
use Balancer::Chash;

use constant {CONSISTENT_POINTS => 160, POW32 => 2**32};

our @EXPORT = qw(
    new
    reinit
    set
    find
    next
);

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
sub _decr($$$);

sub _precompute($) {
    my $nodes = shift;

    my ($n, $total_weight) = (0, 0);
    while (my ($id, $weight) = each(%$nodes)) {
        $n++;
        $total_weight += $weight;
    }

    my $newnodes = $nodes;

    my $ids     = [];
    my $npoints = $total_weight * CONSISTENT_POINTS;
    my $points  = calloc_chash_point_t($npoints);

    my ($start, $index) = (0, 0);
    while (my ($id, $weight) = each(%$nodes)) {
        my $num       = $weight * CONSISTENT_POINTS;
        my $base_hash = crc32($id) ^ 0xffffffff;

        $ids->[$index] = $id;

        $index ++;
        chash_point_init(@$points, $base_hash, $start, $num, $index);

        $start += $num;
    }

    chash_point_sort(@$points, $npoints);

    return ($ids, $points, $npoints, $newnodes);
}

sub new($$) {
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

sub reinit($$) {
    my $self  = shift;
    my $nodes = shift;

    ($self->{ids}, $self->{points}, $self->{npoints}, $self->{newnodes})
        = _precompute($nodes);
    $self->{size} = $self->{npoints};
}

sub _delete($$) {
    my $self = shift;
    my $id   = shift;

    my $nodes      = $self->{nodes};
    my $ids        = $self->{ids};
    my $old_weight = $nodes->{$id};

    return if not $old_weight;

    my $index = 1;

    # find the index: O(n)
    while ($ids->[$index - 1] != $id) {
        $index++;
    }

    # $nodes->{$id} = undef
    # $ids->[$index - 1] = undef
    delete $nodes->{$id};
    $ids->[$index - 1] = undef;

    chash_point_delete(@{$self->{points}}, $self->{npoints}, $index);

    $self->{npoints} -= CONSISTENT_POINTS * $old_weight;
}

sub _incr($$$) {
    my $self = shift;
    my ($id, $weight) = @_;

    $weight = int($weight) || 1;
    my $nodes      = $self->{nodes};
    my $ids        = $self->{ids};
    my $old_weight = $nodes->{$id};

    my $index = 1;
    if ($old_weight) {

        # find the index: O(n)
        while ($ids->[$index - 1] != $id) {
            $index++;
        }
    }
    else {
        $old_weight = 0;

        $index = scalar($ids) + 1;
        $ids->[$index - 1] = $id;
    }

    $nodes->{$id} = $old_weight + $weight;

    my $new_points  = $self->{points};
    my $new_npoints = $self->{new_npoints} + $weight * CONSISTENT_POINTS;
    if ($self->{size} < $new_npoints) {
        $new_points = calloc_chash_point_t($new_npoints);
        $self->{size} = $new_npoints;
    }

    my $base_hash = crc32("${id}") ^ 0xffffffff;
    chash_point_add(
        @{$self->{points}}, $self->{npoints}, $base_hash,
        $old_weight * CONSISTENT_POINTS,
        $weight * CONSISTENT_POINTS,
        $index, @$new_points
    );

    $self->{points}  = $new_points;
    $self->{npoints} = $new_npoints;
}

sub _decr($$$) {
    my $self = shift;
    my ($id, $weight) = @_;

    $weight = int($weight) || 1;
    my $nodes      = $self->{nodes};
    my $ids        = $self->{ids};
    my $old_weight = $nodes->{$id};

    return if not $old_weight;

    if ($old_weight <= $weight) {
        return $self->_delete($id);
    }

    my $index = 1;

    # find the index: O(n)
    while ($ids->[$index - 1] != $id) {
        $index++;
    }

    my $base_hash = crc32("${id}") ^ 0xffffffff;
    chash_point_reduce(
        @{$self->{points}}, $self->{npoints}, $base_hash,
        ($old_weight - $weight) * CONSISTENT_POINTS,
        CONSISTENT_POINTS * $weight, $index
    );

    $nodes->{$id} = $old_weight - $weight;
    $self->{npoints} = $self->{npoints} - CONSISTENT_POINTS * $weight;
}

sub set($$$) {
    my $self = shift;
    my ($id, $new_weight) = @_;

    $new_weight = int($new_weight) || 0;
    my $old_weight = $self->{nodes}->{$id} || 0;

    return 1 if $old_weight == $new_weight;

    return $self->_incr($id, $new_weight - $old_weight)
        if $old_weight < $new_weight;

    return $self->_decr($id, $old_weight - $new_weight);
}

sub _find_id($$$) {
    my ($points, $npoints, $hash) = @_;

    my $step  = POW32 / $npoints;
    my $index = floor($hash / $step);

    my $max_index = $npoints - 1;

    # it seems safer to do this
    if ($index > $max_index) {
        $index = $max_index;
    }

    # find the first points >= hash
    if ($points->[$index]->{hash} >= $hash) {
        for (my $i = $index; $i >= 1; $i--) {
            if ($points->[$i - 1]->{hash} < $hash) {
                return ($points->[$i]->{id}, $i);
            }
        }

        return ($points->[0]->{id}, 0);
    }

    for (my $i = $index + 1; $i <= $max_index; $i++) {
        if ($hash <= $points->[$i]->{hash}) {
            return ($points->[$i]->{id}, $i);
        }
    }

    return ($points->[0]->{id}, 0);
}

sub find($$) {
    my $self = shift;
    my $key  = shift;

    my $hash = crc32("${key}");

    my ($id, $index) = _find_id($self->{points}, $self->{npoints}, $hash);

    return ($self->{ids}->[$id - 1], $index);
}

sub _next($$) {
    my $self  = shift;
    my $index = shift;

    my $new_index = ($index + 1) % $self->npoints;
    my $id        = $self->{points}->[$new_index - 1]->{id};

    return $self->{ids}->[$id - 1], $new_index;
}

1;
