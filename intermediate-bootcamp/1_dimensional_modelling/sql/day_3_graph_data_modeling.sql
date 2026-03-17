-- Graph data modeling (Day 3)
--
-- Purpose:
-- Build a small property-graph in Postgres from the relational NBA dataset so we
-- can query relationships (player↔player matchups, player→game participation)
-- without repeating heavy self-joins on raw fact tables.
--
-- Source tables: games, game_details, teams
--
-- Vertex types: player, team, game
-- Edge types:   plays_in, plays_against, shares_team (plays_on is defined but not loaded here)

-- Step 0: Define enums used to constrain vertex/edge types.
CREATE TYPE vertex_type AS ENUM('player', 'team', 'game');

-- Step 0: Create a single vertices table for all entities.
-- Composite PK (identifier, type) prevents collisions across entity kinds.
-- JSON properties hold type-specific attributes (schema-on-read for this lab).
CREATE TABLE vertices(
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

-- Step 0: Define allowed relationship types.
CREATE TYPE edge_type AS ENUM(
    'plays_against',
    'shares_team',
    'plays_in',
    'plays_on'
);

-- Step 0: Create an edges table for directed relationships (subject → object).
-- The composite PK makes inserts idempotent (no duplicate edges for same pair/type).
CREATE TABLE edges(
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (
        subject_identifier,
        subject_type,
        object_identifier,
        object_type,
        edge_type
    )
);

-- Step 1: Load game vertices (one vertex per game_id).
-- Include a derived winning_team to avoid re-deriving it later.
INSERT INTO vertices
SELECT game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(
        'pts_home',
        pts_home,
        'pts_away',
        pts_away,
        'winning_team',
        CASE
            WHEN home_team_wins = 1 THEN home_team_id
            ELSE visitor_team_id
        END 
    ) AS properties
FROM games;

-- Step 2: Load player vertices by aggregating game_details to player-level.
-- Teams is an array since players can appear for multiple teams.
INSERT INTO vertices
WITH players_agg AS (
    SELECT player_id AS identifier,
        MAX(player_name) AS player_name,
        COUNT(1) AS number_of_games,
        SUM(pts) AS total_points,
        ARRAY_AGG(DISTINCT team_id) AS teams
    FROM game_details
    GROUP BY 1
)

SELECT identifier,
    'player'::vertex_type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points',total_points,
        'teams', teams
    ) AS properties
FROM players_agg;


-- Step 3: Load team vertices (dedupe teams table to one row per team_id).
INSERT INTO vertices
WITH teams_deduped AS (
    SELECT * ,
        ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
    FROM teams
)
SELECT team_id as identifier,
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    ) AS properties
FROM teams_deduped
WHERE row_num = 1;

-- Sanity check: confirm vertices exist for each vertex_type.
SELECT type , COUNT(1)
FROM vertices
GROUP BY 1;

-- Step 4: Create player → game edges (plays_in) from game_details.
-- Deduplicate (player_id, game_id) first to avoid PK violations.
INSERT INTO edges
WITH deduped AS (
    SELECT * ,
        ROW_NUMBER() OVER(PARTITION BY player_id, game_id) as row_num
    FROM game_details
)
SELECT player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    game_id AS object_identifier,
    'game':: vertex_type AS object_type,
    'plays_in' ::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) AS properties
FROM deduped
WHERE row_num = 1;

-- Quick validation: highest single-game points per player from plays_in edges.
SELECT 
    v.properties ->> 'player_name',
    MAX(CAST(e.properties ->> 'pts' AS INTEGER)) 
FROM vertices v
    JOIN edges e ON e.subject_identifier = v.identifier
    AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC;

-- Step 5: Create player ↔ player edges by pairing players who appeared in the same game.
-- We emit each unordered pair once using (f1.player_id > f2.player_id).
-- Edge type:
--   - same team_abbreviation => shares_team
--   - different team_abbreviation => plays_against
-- Then we aggregate across all shared games into one edge per pair + edge_type.
INSERT INTO edges
WITH deduped AS (
    SELECT * ,
        ROW_NUMBER() OVER(PARTITION BY player_id, game_id) as row_num
    FROM game_details
), 
filtered AS (
    SELECT * 
    FROM deduped
    WHERE row_num = 1
),
aggregated AS (
    SELECT f1.player_id AS subject_player_id,
        f2.player_id AS object_player_id,
        CASE
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END AS edge_type,
        MAX(f1.player_name) AS subject_player_name,
        MAX(f2.player_name) AS object_player_name,
        COUNT(1) AS num_games,
        SUM(f1.pts) AS subject_points,
        SUM(f2.pts) AS object_points
    FROM filtered f1
        JOIN filtered f2 ON f1.game_id = f2.game_id
        AND f1.player_name <> f2.player_name
    WHERE f1.player_id > f2.player_id
    GROUP BY f1.player_id,
        f2.player_id,
        CASE
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END
)
SELECT subject_player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    object_player_id AS object_identifier,
    'player'::vertex_type AS object_type,
    edge_type AS edge_type,
    json_build_object(
        'num_games',
        num_games,
        'subject_points',
        subject_points,
        'object_points',
        object_points
    )
FROM aggregated;

-- -----------------------------------------------------------------------------
-- Queries (examples)
-- -----------------------------------------------------------------------------

-- Example 1 (exploratory): join player vertices to player→player edges.
-- NOTE: This version computes games/points (inverted). Kept here as the first draft.
SELECT v.properties->>'player_name' AS player_name,
    e.object_identifier,
    COALESCE(CAST(v.properties->>'number_of_games' AS real), 0)/
    CASE WHEN CAST(v.properties->>'total_points' AS real) =  0 THEN 1 ELSE CAST(v.properties->>'total_points' AS real) END,
    e.properties ->> 'subject_points',
    e.properties ->> 'num_games'
FROM vertices v
    JOIN edges e ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;

-- Example 2 (final): compute points_per_game correctly and cast JSON once in a CTE.
-- NULLIF(...,'') avoids cast errors if any JSON fields are empty strings.
-- NULLIF(total_points, 0) avoids divide-by-zero.
WITH typed AS (
  SELECT
    v.identifier,
    v.type,
    v.properties->>'player_name' AS player_name,
    NULLIF(v.properties->>'number_of_games','')::numeric AS number_of_games,
    NULLIF(v.properties->>'total_points','')::numeric     AS total_points,
    e.object_identifier,
    NULLIF(e.properties->>'subject_points','')::numeric   AS subject_points,
    NULLIF(e.properties->>'num_games','')::numeric        AS num_games
  FROM vertices v
  JOIN edges e
    ON v.identifier = e.subject_identifier
   AND v.type = e.subject_type
  WHERE e.object_type = 'player'::vertex_type
)
SELECT
  player_name,
  object_identifier,
  COALESCE(NULLIF(total_points, 0) / number_of_games , 0) AS points_per_game,
  subject_points,
  num_games
FROM typed;
