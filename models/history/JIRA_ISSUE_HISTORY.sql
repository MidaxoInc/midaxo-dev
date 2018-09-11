with  datespine as
        (select *
        from MIDAXO.DEV.DATETABLE_CLEAN ),
      sprintdate as
        (select d.ddate,
                array_agg(s.id) within GROUP (order by d.ddate) as active_sprints_id
        from MIDAXO.DEV.DATETABLE_CLEAN d
        left join raw.jira.sprint s
        on d.ddate between to_date(s.start_date) and to_date(s.end_date)
        group by d.ddate),
      sprinthist_raw as
        (select time as ddate,
                issue_id,
                sprint_id,
                row_number() over (partition by issue_id order by ddate asc) as x
        from raw.jira.issue_sprint_history),
      sprint as
        (select a.issue_id,
                a.sprint_id,
                a.ddate as validfrom,
                case  when b.ddate is null then current_date
                      else b.ddate end as validto,
                a.ddate = max(a.ddate) over (partition by a.issue_id, to_date(a.ddate)) as valid
          from sprinthist_raw a
          left join sprinthist_raw b
          on a.issue_id = b.issue_id
          and a.x + 1 = b.x),
      statushist_raw as
      (select time as ddate,
              issue_id,
              status_id,
              row_number() over (partition by issue_id order by ddate asc) as x
      from raw.jira.issue_status_history),
      status as
        (select a.issue_id,
                a.status_id,
                a.ddate as validfrom,
                case  when b.ddate is null then current_date
                      else b.ddate end as validto,
                a.ddate = max(a.ddate) over (partition by a.issue_id, to_date(a.ddate)) as valid
          from statushist_raw a
          left join statushist_raw b
          on a.issue_id = b.issue_id
          and a.x + 1 = b.x),
      storyhist_raw as
      (select time as ddate,
              issue_id,
              value,
              row_number() over (partition by issue_id order by ddate asc) as x
      from raw.jira.issue_story_points_history),
      story as
        (select a.issue_id,
                a.value as story_points,
                a.ddate as validfrom,
                case  when b.ddate is null then current_date
                      else b.ddate end as validto,
                a.ddate = max(a.ddate) over (partition by a.issue_id, to_date(a.ddate)) as valid
          from storyhist_raw a
          left join storyhist_raw b
          on a.issue_id = b.issue_id
          and a.x + 1 = b.x),
      issue as
        (select a.id as issue_id,
                a.key,
                a.issue_type as type_id,
                a.project as project_id,
                a.epic_link as epic_id,
                a.priority as priority_id,
                to_date(a.created) as createdate,
                a.description,
                a.assignee
        from raw.jira.issue a)
-----------------------------------------------------------
select  d.ddate,
        d.active_sprints_id,
        i.*,
        sprint.sprint_id,
        status.status_id,
        story.story_points
from sprintdate d
left join issue i
  on  i.createdate <= d.ddate
  left join sprint
    on  i.issue_id = sprint.issue_id
        and d.ddate between to_date(sprint.validfrom) and to_date(sprint.validto)
        and sprint.valid = true
left join status
  on  i.issue_id = status.issue_id
      and d.ddate between to_date(status.validfrom) and to_date(status.validto)
      and status.valid = true
left join story
  on  i.issue_id = story.issue_id
      and d.ddate between to_date(story.validfrom) and to_date(story.validto)
      and story.valid = true
where d.ddate between to_date('2017-01-01') and current_date
order by d.ddate desc
