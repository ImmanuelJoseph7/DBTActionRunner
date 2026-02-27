with stats as (
    select * from {{ source('pfdatalake', 'stats') }}
),

teams as (
    select * from {{ source('pfdatalake', 'teams') }}
),

aggregated as (
    select
        s.season,
        s.team,
        sum(s.assists) as assists,
        sum(s.goals_scored) as goals
    from stats as s
    inner join teams as t
        on s.season = t.season
        and s.opponent_team = t.id
    group by s.season, s.team
)

select * from aggregated
