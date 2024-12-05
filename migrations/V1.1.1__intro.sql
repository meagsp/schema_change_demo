create or replace view DEV_CANVAS_DB.STG_CANVAS_SCH.VW_CANVAS_ACCESS_ACTIVITY(
	"request id",
	"session id",
	"user id",
	"site id",
	"day of access",
	"time of access",
	"timestamp of access",
	"hour of access",
	"url accessed",
	"http user agent",
	"ip address",
	"web application controller",
	"web application action",
	"http method",
	"http version",
	"http status code",
	"http user agent - formating"
) as
WITH transformed_logs AS (
    SELECT
        rq.id AS "request id",
        rq.session_id AS "session id",
        rq.user_id::varchar as user_id,
        rq.course_id,
        convert_timezone('Australia/Sydney', rq.timestamp) AS local_timestamp,
        rq.url,
        rq.user_agent,
        rq.remote_ip,
        rq.web_application_controller,
        rq.web_application_action,
        rq.http_method,
        rq.http_version,
        rq.http_status
    FROM PRD_CANVAS_DB.CANVAS_SCH.WEB_LOGS rq
    WHERE rq.web_application_action <> 'ping'
    --AND to_date(convert_timezone('Australia/Sydney', rq.timestamp)) between to_date('2024801', 'YYYYMMDD') AND to_date('20241031', 'YYYYMMDD')
    --AND to_date(convert_timezone('Australia/Sydney', rq.timestamp))= to_date('20241101', 'YYYYMMDD')
),
processed_logs AS (
    SELECT
        tl."request id",
        tl."session id",
        ud.id::varchar AS "user id",
        cd.id AS "site id",
        to_date(tl.local_timestamp) AS "day of access",
        CAST(tl.local_timestamp AS TIME) AS "time of access", -- Output as a TIME field
        to_char(tl.local_timestamp, 'HH24:MI:SS.FF3') AS "timestamp of access",
        extract(hour FROM tl.local_timestamp) AS "hour of access",
        tl.url AS "url accessed",
        tl.user_agent AS "http user agent",
        tl.remote_ip AS "ip address",
        tl.web_application_controller AS "web application controller",
        tl.web_application_action AS "web application action",
        tl.http_method AS "http method",
        tl.http_version AS "http version",
        tl.http_status AS "http status code",
        CASE 
            WHEN LOWER(tl.user_agent) LIKE '%android%' THEN 1
            WHEN LOWER(tl.user_agent) LIKE '%ios%' THEN 1
            WHEN LOWER(tl.user_agent) LIKE '%iphone%' THEN 1
            WHEN LOWER(tl.user_agent) LIKE '%ipad%' THEN 1
            WHEN LOWER(tl.user_agent) LIKE '%mobile%' THEN 1
            ELSE 0
        END AS "http user agent - formating"
    FROM transformed_logs tl
    INNER JOIN PRD_CANVAS_DB.CANVAS_SCH.USERS ud ON tl.user_id = ud.id
    INNER JOIN PRD_CANVAS_DB.CANVAS_SCH.COURSES cd ON tl.course_id = cd.id
)
SELECT
    "request id",
    "session id",
    "user id",
    "site id" ,
    "day of access",
    "time of access",
    "timestamp of access",
    "hour of access",
    "url accessed",
    "http user agent",
    "ip address",
    "web application controller",
    "web application action",
    "http method",
    "http version",
    "http status code",
    "http user agent - formating"
FROM processed_logs

;
