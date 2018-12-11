SELECT a.id,
       to_date(a.created) AS createdate,
       a.issue_type,
       a.key,
       f.name as affected_version,
       a.priority||' - ' || d.name AS priority,
       c.name AS status_detail,
       CASE
           WHEN c.name in ('Done',
                           'Closed') then 'done'
           ELSE 'open'
       END AS status,
       b.name AS TYPE,
       case when to_date(a.created) <= to_date(f.release_date) then 'pre-release'
            when f.release_date is null then 'pre-release'
            else 'post-release' end as bug_type
FROM raw.jira.issue a
left join raw.jira.issue_type b
  ON a.issue_type = b.id
left join raw.jira.status c
  ON c.id = a.status
left join RAW.JIRA.PRIORITY d
  ON d.id = a.priority
left join RAW.JIRA.ISSUE_AFFECTS_VERSION_S e
  ON e.issue_id = a.id
left join RAW.JIRA.VERSION f
  ON e.version_id = f.id
  WHERE b.name = 'Bug'
    and a.priority in ('1','2')
    and c.name not in ('Done',
                           'Closed')
ORDER BY a.priority DESC
