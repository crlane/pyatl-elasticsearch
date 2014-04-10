from pyes import ES
from pyes import filters as F
from pyes import query as Q
from pprint import pprint as pp

_all = Q.MatchAllQuery()
conn = ES(('http', '127.0.0.1', '9200'))

def team_from_state(team, state):
    state = F.QueryFilter(Q.MatchQuery('birth_place', state))
    team = F.QueryFilter(Q.MatchQuery('appearances.franchise', team))
    bool_filter = F.BoolFilter()
    for filt in (state, team):
        bool_filter.add_must(filt)
    fq = Q.FilteredQuery(_all, bool_filter)
    pp(fq.serialize())
    raw_input('press enter to continue...')
    rs = conn.search(fq)
    print rs.total
    for player in rs:
        for name in player.names:
            if name.type_ == 'common':
                common_name = name
                break
        print '{} from {}'.format(common_name['name'], player['birth_place'])

def only_pitchers():
    ef = F.ExistsFilter('appearances.stats.pitching')
