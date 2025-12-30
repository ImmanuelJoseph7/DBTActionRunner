with gps as
(SELECT  s.season, s.name, s.team, SUM(s.assists) AS assists, SUM(s.goals_scored) AS goals, SUM(s.minutes) AS MinutesPlayed
FROM   pfdatalake.stats AS s INNER JOIN
             pfdatalake.teams AS t ON s.season = t.season AND s.opponent_team = t.id
GROUP BY s.season, s.name, s.team)

select * from gps