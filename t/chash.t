use strict;
use warnings;
use Test::More;
use lib './lib';

require_ok("Resty::Balancer::Consistance");

# TEST 1: find
# ===

{
    # --- config
    my $servers = {
        server1 => 10,
        server2 => 2,
        server3 => 1
    };

    my $chash = Resty::Balancer::Consistance->new($servers);

    my $res = {};

    for my $i (1..100000) {
        my ($id) = $chash->find($i);

        $res->{$id} ++;
    }

    # --- response_body
    ok($res->{server2} == 14743 && $res->{server1} == 77075 && $res->{server3} == 8182);
    ok($chash->{npoints} == 2080);
}

# TEST 2: compare with nginx chash
# ===
# --- SKIP

# TEST 3: next
# ===
{
    # --- config
    my $servers = {
        server1 => 2,
        server2 => 2,
        server3 => 1
    };

    my $chash = Resty::Balancer::Consistance->new($servers);

    my ($id, $idx) = $chash->find("foo");

    ok($id eq "server1" && $idx == 434);

    for my $i (1..100) {
        ($id, $idx) = $chash->_next($idx);
    }

    ok($id eq "server2" && $idx == 534);
}

# TEST 4: up, decr
# ===
{
    my $servers = {
        server1 => 7,
        server2 => 2,
        server3 => 1
    };

    my $chash = Resty::Balancer::Consistance->new($servers);

    my $num = 100 * 1000;

    my $res1 = {};
    for my $i (1..$num) {
        my ($id) = $chash->find($i);

        $res1->{$i} = $id;
    }

    $chash->incr("server1");

    my $res2 = {};
    for my $i (1..$num) {
        my ($id) = $chash->find($i);

        $res2->{$i} = $id
    }

    my ($same, $diff) = (0, 0);
    for my $i (1..$num) {
        if ($res1->{$i} eq $res2->{$i}) {
            $same ++;
        } else {
            $diff ++;
        }
    }

    ok($same == 97606 && $diff == 2394);

    $chash->decr('server3');

    my $res3 = {};
    for my $i (1..$num) {
        my ($id) = $chash->find($i);

        $res3->{$i} = $id;
    }

    ($same, $diff) = (0, 0);
    for my $i (1..$num) {
        if ($res3->{$i} eq $res2->{$i}) {
            $same ++;
        } else {
            $diff ++;
        }
    }

    ok($same == 90255 && $diff == 9745);
}

done_testing();
