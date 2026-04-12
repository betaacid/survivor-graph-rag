from lib.neo4j_client import run_write


def upsert_season(props):
    run_write("""
        MERGE (s:Season {title: $title})
        SET s += $props
    """, {"title": props["title"], "props": props})


def upsert_player(name):
    run_write("MERGE (p:Player {name: $name})", {"name": name})


def upsert_player_season(player_name, season_number, props):
    run_write("""
        MERGE (ps:PlayerSeason {player_name: $player_name, season_number: $season_number})
        SET ps += $props
        WITH ps
        MERGE (p:Player {name: $player_name})
        MERGE (p)-[:PLAYED_IN]->(ps)
        WITH ps
        MATCH (s:Season {number: $season_number})
        MERGE (ps)-[:IN_SEASON]->(s)
    """, {"player_name": player_name, "season_number": season_number, "props": props})


def upsert_tribe(name, season_number, phase="premerge"):
    run_write("""
        MERGE (t:Tribe {name: $name, season_number: $season_number})
        SET t.phase = $phase
        WITH t
        MATCH (s:Season {number: $season_number})
        MERGE (s)-[:HAS_TRIBE]->(t)
    """, {"name": name, "season_number": season_number, "phase": phase})


def link_player_tribe(player_name, season_number, tribe_name):
    run_write("""
        MATCH (ps:PlayerSeason {player_name: $player_name, season_number: $season_number})
        MATCH (t:Tribe {name: $tribe_name, season_number: $season_number})
        MERGE (ps)-[:MEMBER_OF]->(t)
    """, {"player_name": player_name, "season_number": season_number, "tribe_name": tribe_name})


def upsert_episode(season_number, episode_number, props):
    run_write("""
        MERGE (e:Episode {season_number: $season_number, episode_number: $ep_num})
        SET e += $props
        WITH e
        MATCH (s:Season {number: $season_number})
        MERGE (s)-[:HAS_EPISODE]->(e)
    """, {"season_number": season_number, "ep_num": episode_number, "props": props})


def link_episode_immunity(season_number, episode_number, winner_name):
    run_write("""
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MATCH (ps:PlayerSeason {player_name: $name, season_number: $sn})
        MERGE (e)-[:IMMUNITY_WON_BY]->(ps)
    """, {"sn": season_number, "ep": episode_number, "name": winner_name})


def link_episode_reward(season_number, episode_number, winner_name):
    run_write("""
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MATCH (ps:PlayerSeason {player_name: $name, season_number: $sn})
        MERGE (e)-[:REWARD_WON_BY]->(ps)
    """, {"sn": season_number, "ep": episode_number, "name": winner_name})


def link_episode_eliminated(season_number, episode_number, player_name):
    run_write("""
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MATCH (ps:PlayerSeason {player_name: $name, season_number: $sn})
        MERGE (e)-[:ELIMINATED]->(ps)
    """, {"sn": season_number, "ep": episode_number, "name": player_name})


def add_vote(voter_name, target_name, season_number, episode_number):
    run_write("""
        MATCH (voter:PlayerSeason {season_number: $sn})
        WHERE voter.player_name = $voter OR voter.player_name STARTS WITH $voter
        WITH voter LIMIT 1
        MATCH (target:PlayerSeason {season_number: $sn})
        WHERE target.player_name = $target OR target.player_name STARTS WITH $target
        WITH voter, target LIMIT 1
        MERGE (voter)-[v:CAST_VOTE {episode_number: $ep}]->(target)
    """, {"voter": voter_name, "target": target_name, "sn": season_number, "ep": episode_number})


def upsert_tribal_council(season_number, episode_number):
    run_write("""
        MERGE (tc:TribalCouncil {season_number: $sn, episode_number: $ep})
        WITH tc
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MERGE (e)-[:HAS_TRIBAL]->(tc)
    """, {"sn": season_number, "ep": episode_number})


def link_tribal_attendee(season_number, episode_number, player_name):
    run_write("""
        MATCH (ps:PlayerSeason {season_number: $sn})
        WHERE ps.player_name = $name OR ps.player_name STARTS WITH $name
        WITH ps LIMIT 1
        MATCH (tc:TribalCouncil {season_number: $sn, episode_number: $ep})
        MERGE (ps)-[:ATTENDED_TRIBAL]->(tc)
    """, {"name": player_name, "sn": season_number, "ep": episode_number})


def add_jury_vote(juror_name, voted_for_name, season_number):
    run_write("""
        MATCH (juror:PlayerSeason {season_number: $sn})
        WHERE juror.player_name = $juror OR juror.player_name STARTS WITH $juror
        MATCH (winner:PlayerSeason {season_number: $sn})
        WHERE winner.player_name = $voted_for OR winner.player_name STARTS WITH $voted_for
        WITH juror, winner LIMIT 1
        MERGE (juror)-[:JURY_VOTE_FOR]->(winner)
    """, {"juror": juror_name, "voted_for": voted_for_name, "sn": season_number})


def link_episode_tribe(season_number, episode_number, tribe_name):
    run_write("""
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MATCH (t:Tribe {name: $tribe, season_number: $sn})
        MERGE (e)-[:TRIBAL_COUNCIL_FOR]->(t)
    """, {"sn": season_number, "ep": episode_number, "tribe": tribe_name})
