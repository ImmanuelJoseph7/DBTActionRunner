with gpspt as
(SELECT  s.season,  s.team, SUM(s.assists) AS assists, SUM(s.goals_scored) AS goals
FROM   pfdatalake.stats AS s INNER JOIN
             pfdatalake.teams AS t ON s.season = t.season AND s.opponent_team = t.id
GROUP BY s.season, s.team
)

select * from gpspt