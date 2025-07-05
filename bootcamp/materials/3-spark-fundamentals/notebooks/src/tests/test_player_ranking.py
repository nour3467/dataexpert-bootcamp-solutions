from jobs.player_ranking_job import do_player_ranking_transformation
from collections import namedtuple

PlayerMatch = namedtuple("PlayerMatch", "player_gamertag player_total_kills player_total_deaths")


def test_player_ranking_with_skill_tiers(spark):
    # Create test input data
    input_data = [
        # Elite player - avg kills > 20
        PlayerMatch("ProGamer", 25, 5),
        PlayerMatch("ProGamer", 22, 8),
        PlayerMatch("ProGamer", 28, 3),
        PlayerMatch("ProGamer", 20, 6),
        PlayerMatch("ProGamer", 30, 4),

        # Good player - avg kills between 10-20
        PlayerMatch("GoodPlayer", 15, 10),
        PlayerMatch("GoodPlayer", 12, 12),
        PlayerMatch("GoodPlayer", 18, 8),
        PlayerMatch("GoodPlayer", 14, 11),
        PlayerMatch("GoodPlayer", 16, 9),

        # Beginner player - avg kills < 10
        PlayerMatch("Newbie", 8, 15),
        PlayerMatch("Newbie", 6, 18),
        PlayerMatch("Newbie", 9, 12),
        PlayerMatch("Newbie", 7, 16),
        PlayerMatch("Newbie", 5, 20),

        # Player with < 5 games (should be filtered out)
        PlayerMatch("InactivePlayer", 20, 5),
        PlayerMatch("InactivePlayer", 25, 3),

        # Player with NULL kills (should be filtered out)
        PlayerMatch("NullPlayer", None, 10),
    ]

    source_df = spark.createDataFrame(input_data)
    actual_df = do_player_ranking_transformation(spark, source_df)

    print("✅ Query executed successfully!")
    actual_df.show()

    # Collect results for validation
    results = actual_df.collect()

    # Should have exactly 3 players (filtered out inactive and null players)
    assert len(results) == 3, f"Expected 3 rows, got {len(results)}"

    # Find each player's results
    pro_gamer = [r for r in results if r.player_gamertag == "ProGamer"][0]
    good_player = [r for r in results if r.player_gamertag == "GoodPlayer"][0]
    newbie = [r for r in results if r.player_gamertag == "Newbie"][0]

    # Validate ProGamer (Elite tier)
    assert pro_gamer.avg_kills == 25.0, f"ProGamer avg kills should be 25.0, got {pro_gamer.avg_kills}"
    assert pro_gamer.avg_deaths == 5.2, f"ProGamer avg deaths should be 5.2, got {pro_gamer.avg_deaths}"
    assert pro_gamer.games_played == 5, f"ProGamer games should be 5, got {pro_gamer.games_played}"
    assert pro_gamer.kill_rank == 1, f"ProGamer should be rank 1, got {pro_gamer.kill_rank}"
    assert pro_gamer.skill_tier == "Elite", f"ProGamer should be Elite, got {pro_gamer.skill_tier}"

    # Validate GoodPlayer (Good tier)
    assert good_player.avg_kills == 15.0, f"GoodPlayer avg kills should be 15.0, got {good_player.avg_kills}"
    assert good_player.avg_deaths == 10.0, f"GoodPlayer avg deaths should be 10.0, got {good_player.avg_deaths}"
    assert good_player.games_played == 5, f"GoodPlayer games should be 5, got {good_player.games_played}"
    assert good_player.kill_rank == 2, f"GoodPlayer should be rank 2, got {good_player.kill_rank}"
    assert good_player.skill_tier == "Good", f"GoodPlayer should be Good, got {good_player.skill_tier}"

    # Validate Newbie (Beginner tier)
    assert newbie.avg_kills == 7.0, f"Newbie avg kills should be 7.0, got {newbie.avg_kills}"
    assert newbie.avg_deaths == 16.2, f"Newbie avg deaths should be 16.2, got {newbie.avg_deaths}"
    assert newbie.games_played == 5, f"Newbie games should be 5, got {newbie.games_played}"
    assert newbie.kill_rank == 3, f"Newbie should be rank 3, got {newbie.kill_rank}"
    assert newbie.skill_tier == "Beginner", f"Newbie should be Beginner, got {newbie.skill_tier}"

    # Verify required columns exist
    expected_columns = ["player_gamertag", "avg_kills", "avg_deaths", "games_played", "kill_rank", "skill_tier"]
    actual_columns = actual_df.columns

    for col in expected_columns:
        assert col in actual_columns, f"Missing column: {col}"

    print("✅ All validations passed!")
    print(f"✅ ProGamer: {pro_gamer.avg_kills} avg kills, rank {pro_gamer.kill_rank}, {pro_gamer.skill_tier} tier")
    print(
        f"✅ GoodPlayer: {good_player.avg_kills} avg kills, rank {good_player.kill_rank}, {good_player.skill_tier} tier")
    print(f"✅ Newbie: {newbie.avg_kills} avg kills, rank {newbie.kill_rank}, {newbie.skill_tier} tier")
    print("✅ PostgreSQL to PySpark ranking conversion test PASSED!")