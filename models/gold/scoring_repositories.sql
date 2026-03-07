{{ config(materialized='table') }}

with recent_activity as (
    select
        repo_id,
        sum(commits_count) as recent_commits,
        sum(unique_committers) as recent_contributors,
        sum(prs_merged) as recent_merged_prs,
        avg(avg_pr_close_hours) as recent_avg_pr_close_hours,
        avg(avg_issue_close_hours) as recent_avg_issue_close_hours
    from {{ ref('fact_repo_activity') }}
    where activity_date >= current_date - interval '30 days'
    group by repo_id
),

community_history as (
    select
        repo_id,
        sum(prs_opened) as total_prs,
        sum(prs_merged) as merged_prs,
        sum(issues_opened) as total_issues,
        sum(issues_closed) as closed_issues
    from {{ ref('fact_repo_activity') }}
    group by repo_id
),

base_metrics as (
    select
        d.repo_id,
        d.stars_count,
        d.forks_count,
        d.watchers_count,
        coalesce(r.recent_commits, 0) as recent_commits,
        coalesce(r.recent_contributors, 0) as recent_contributors,
        coalesce(r.recent_merged_prs, 0) as recent_merged_prs,
        r.recent_avg_pr_close_hours,
        r.recent_avg_issue_close_hours,
        coalesce(c.total_prs, 0) as total_prs,
        coalesce(c.merged_prs, 0) as merged_prs,
        coalesce(c.total_issues, 0) as total_issues,
        coalesce(c.closed_issues, 0) as closed_issues,
        coalesce(c.merged_prs * 1.0 / nullif(c.total_prs, 0), 0) as pr_merge_rate,
        coalesce(c.closed_issues * 1.0 / nullif(c.total_issues, 0), 0) as issue_close_rate
    from {{ ref('dim_repository') }} d
    left join recent_activity r using (repo_id)
    left join community_history c using (repo_id)
),

ranked as (
    select
        *,
        ntile(10) over (order by stars_count) as rank_stars,
        ntile(10) over (order by forks_count) as rank_forks,
        ntile(10) over (order by watchers_count) as rank_watchers,

        ntile(10) over (order by recent_commits) as rank_recent_commits,
        ntile(10) over (order by recent_contributors) as rank_recent_contributors,
        ntile(10) over (order by recent_merged_prs) as rank_recent_merged_prs,

        ntile(10) over (order by recent_avg_pr_close_hours desc) as rank_pr_close,
        ntile(10) over (order by recent_avg_issue_close_hours desc) as rank_issue_close,

        ntile(10) over (order by pr_merge_rate) as rank_pr_merge_rate,
        ntile(10) over (order by issue_close_rate) as rank_issue_close_rate
    from base_metrics
),

scored as (
    select
        repo_id,

        -- 3 metrics => max possible = 30
        (rank_stars + rank_forks + rank_watchers) * 100.0 / 30 as popularity_score,

        -- 3 metrics => max possible = 30
        (rank_recent_commits + rank_recent_contributors + rank_recent_merged_prs) * 100.0 / 30 as activity_score,

        -- 2 metrics => max possible = 20
        (rank_pr_close + rank_issue_close) * 100.0 / 20 as responsiveness_score,

        -- 2 metrics => max possible = 20
        (rank_pr_merge_rate + rank_issue_close_rate) * 100.0 / 20 as community_score

    from ranked
)

select
    repo_id,
    popularity_score as score_popularity,
    activity_score as score_activity,
    responsiveness_score as score_responsiveness,
    community_score as score_community,
    (
        0.2 * popularity_score +
        0.3 * activity_score +
        0.3 * responsiveness_score +
        0.2 * community_score
    ) as score_global,
    rank() over (
        order by (
            0.2 * popularity_score +
            0.3 * activity_score +
            0.3 * responsiveness_score +
            0.2 * community_score
        ) desc
    ) as ranking
from scored
order by ranking