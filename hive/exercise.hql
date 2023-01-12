create database if not exists exercise;
use exercise;

DROP TABLE IF EXISTS `student`;
CREATE TABLE `student` (
`s_id` varchar(20),
`s_name` varchar(20),
`s_birth` varchar(20),
`s_sex` varchar(10)
)
row format delimited
fields terminated by ','
lines terminated by '\n';
 
load data local inpath '/opt/install/data/student.csv' into table student;

DROP TABLE IF EXISTS `course`;
CREATE TABLE `course` (
  `c_id` varchar(20),
  `c_name` varchar(20),
  `t_id` varchar(20)
)
row format delimited
fields terminated by ','
lines terminated by '\n';
 
load data local inpath '/opt/install/data/course.csv' into table course;

DROP TABLE IF EXISTS `teacher`;
CREATE TABLE `teacher`(
`t_id` varchar(20),
`t_name` varchar(20)
)
row format delimited
fields terminated by ','
lines terminated by '\n';
 
load data local inpath '/opt/install/data/teacher.csv' into table teacher;

DROP TABLE IF EXISTS `score`;
CREATE TABLE `score`(
`s_id` varchar(20),
`c_id` varchar(20),
`s_score` int
)
row format delimited
fields terminated by ','
lines terminated by '\n';
 
load data local inpath '/opt/install/data/score.csv' into table score;

# 查询"01"课程比"02"课程成绩高的学生的信息及课程分数
select
  student.*
  , joined_score.*
from
  student
  join (
    select
      score_01.s_id, score_01.s_score as score01, score_02.s_score as score02
    from
      (select * from score where c_id = '01') score_01
      join (select * from score where c_id = '02') score_02
        on score_01.s_id = score_02.s_id and score_01.s_score > score_02.s_score
  ) joined_score
  on student.s_id = joined_score.s_id
;


# 查询"01"课程比"02"课程成绩低的学生的信息及课程分数
select
  student.*
  , joined_score.*
from
  student
  join (
    select
      score_01.s_id, score_01.s_score as score01, score_02.s_score as score02
    from
      (select * from score where c_id = '01') score_01
      join (select * from score where c_id = '02') score_02
        on score_01.s_id = score_02.s_id and score_01.s_score < score_02.s_score
  ) joined_score
  on student.s_id = joined_score.s_id
;

# 查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩
select
  student.s_id
  , student.s_name
  , avg(score.s_score)
from
  student
  left join score
    on student.s_id = score.s_id
group by
  student.s_id, student.s_name
having
  avg(score.s_score) >= 60
;

# 查询平均成绩小于 60 分的同学的学生编号和学生姓名和平均成绩 (包括有成绩的和无成绩的)
select
  student.s_id
  , student.s_name
  , avg(score.s_score) as avg_score
from
  student
  left join score
    on student.s_id = score.s_id
group by
  student.s_id, student.s_name
having
  avg_score < 60 or avg_score is null
;

# 查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
select
  student.s_id
  , student.s_name
  , count(score.c_id) as course_count
  , sum(score.s_score) as total_score
from
  student
  left join score
    on student.s_id = score.s_id
group by
  student.s_id, student.s_name
;

# 查询"李"姓老师的数量
select
  count(t_id)
from
  teacher
where
  t_name like '李%'
;

# 查询学过"张三"老师授课的同学的信息
select
  *
from
  student
where s_id in (
  select s_id from score where c_id in (
      select c_id from course where t_id in (
        select t_id from teacher where t_name = '张三'
      )
  )
)
;


# 查询没学过"张三"老师授课的同学的信息
select
  *
from
  student
where s_id not in (
  select s_id from score where c_id in (
      select c_id from course where t_id in (
        select t_id from teacher where t_name = '张三'
      )
  )
)
;

# 查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
select
  *
from
  student
where
  s_id in (
    select s_id from score where c_id in ('01', '02') group by s_id having count(c_id) = 2
  )
;

# 查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
select
  *
from
  student
where
  s_id in (
    select s_id from score group by s_id having array_contains(collect_set(cast(c_id as string)), '01') and !array_contains(collect_set(cast(c_id as string)), '02')
  )
;

# 查询没有学全所有课程的同学的信息
select
  student.*
from
  student
  left join (select s_id, count(distinct c_id) as enrolled_course_cnt from score group by s_id) t1
    on student.s_id = t1.s_id
  join (select count(distinct c_id) as total_course_cnt from course) t2
where 
  t1.enrolled_course_cnt is null
  or t1.enrolled_course_cnt < t2.total_course_cnt
;  


# 查询至少有一门课与学号为"01"的同学所学相同的同学的信息
select
  *
from
  student
where
  s_id in (
    select 
      t1.s_id
    from
      (select * from score where s_id != '01') t1
      join (select c_id from score where s_id = '01') t2
        on t1.c_id = t2.c_id
    group by
      t1.s_id
    having count(t2.c_id) >= 1
  )
;

# 查询和"01"号的同学学习的课程完全相同的其他同学的信息
select
  *
from
  student
where
  s_id in (
    select
      t1.s_id
    from
      (select s_id, concat_ws(",", sort_array(collect_set(score.c_id))) as courses_string from score where s_id != '01' group by s_id) t1
      join (select concat_ws(",", sort_array(collect_set(score.c_id))) as courses_string from score where s_id = '01') t2
        on t1.courses_string = t2.courses_string
  )
;

# 查询没学过"张三"老师讲授的任一门课程的学生姓名
select
  s_id
  , s_name
from
  student
where
  s_id not in (
    select distinct s_id from score where c_id in (
      select c_id from course where t_id in (
        select t_id from teacher where t_name = '张三'
      )
    )
  )
;

# 查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
select
  student.s_id
  , student.s_name
  , t1.avg_score
from
  student
  join (
    select
     s_id
     , avg(s_score) as avg_score
    from
      score
    where
      s_id in (
        select s_id from score where s_score < 60 group by s_id having count(c_id) >= 2
      )
    group by
      s_id
  ) t1
    on student.s_id = t1.s_id
;