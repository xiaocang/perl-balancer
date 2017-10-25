use strict;
use warnings;
use Test::More;
use lib './lib';
use Smart::Comments;

require_ok("Balancer::Consistance");

# TEST 1: find
# ===

# --- config
my $servers = {
    server1 => 10,
    server2 => 2,
    server3 => 1
};

my $chash = Balancer::Consistance->new($servers);

my $res = {};

for my $i (1..100000) {
    my ($id) = $chash->find($i);

    $res->{$id} ++;
}
### $res

# # --- response_body
# ok($res->{server2} == 14743);
# ok($res->{server1} == 77075);
# ok($res->{server3} == 8182);
# ok($chash->npoints == 2080);

done_testing();
