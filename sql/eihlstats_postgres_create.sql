CREATE TABLE "public.team" (
	"team_id" serial NOT NULL,
	"name" varchar(255) NOT NULL,
	"date_formed" DATE,
	"arena_id" int,
	CONSTRAINT "team_pk" PRIMARY KEY ("team_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.arena" (
	"arena_id" serial NOT NULL,
	"address" varchar(500),
	"capacity" int,
	CONSTRAINT "arena_pk" PRIMARY KEY ("arena_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.player" (
	"player_id" serial NOT NULL,
	"name" varchar(300) NOT NULL,
	"dob" DATE,
	"nationality" varchar(255),
	"position" varchar(2),
	CONSTRAINT "player_pk" PRIMARY KEY ("player_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.player_team" (
	"team_id" int NOT NULL,
	"player_id" int NOT NULL,
	"date_from" DATE NOT NULL,
	"date_to" DATE
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.match" (
	"match_id" serial NOT NULL,
	"championship_id" int NOT NULL,
	"home_team_id" int NOT NULL,
	"away_team_id" int NOT NULL,
	"location_id" int,
	"match_date" DATE NOT NULL,
	"home_score" int,
	"away_score" int,
	CONSTRAINT "match_pk" PRIMARY KEY ("match_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.match_events" (
	"match_events_id" serial NOT NULL,
	"match_id" varchar(500) NOT NULL,
	"timestamp" TIMESTAMP NOT NULL,
	"comment" varchar(500) NOT NULL,
	CONSTRAINT "match_events_pk" PRIMARY KEY ("match_events_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.goals" (
	"goal_id" serial NOT NULL,
	"match_id" bigint NOT NULL,
	"timestamp" TIME NOT NULL,
	"goalscorer_id" int NOT NULL,
	CONSTRAINT "goals_pk" PRIMARY KEY ("goal_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.goal_assists" (
	"assist_player_id" int NOT NULL,
	"goal_id" int NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.championship" (
	"champion_id" serial NOT NULL,
	"name" varchar(400) NOT NULL UNIQUE,
	"start_date" DATE,
	"end_date" DATE,
	CONSTRAINT "championship_pk" PRIMARY KEY ("champion_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.match_player_stats" (
	"match_id" int NOT NULL,
	"team_id" int NOT NULL,
	"player_id" int NOT NULL,
	"goals" int,
	"assists" int,
	"points" int,
	"penalty_mins" int,
	"power_play_goals" int,
	"short_hand_goals" int,
	"+/-" int,
	"shots" int,
	"shots_on_goal" int,
	"face_offs_won" int,
	"face_offs_lost" int
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.match_team_stats" (
	"match_id" bigint NOT NULL,
	"team_id" bigint NOT NULL,
	"shots" int,
	"shots_on_goal" int,
	"shot_efficiency" DECIMAL(3),
	"power_plays" int,
	"power_play_efficiency" DECIMAL(3),
	"penalty_minutes" int,
	"penalty_kill_efficiency" DECIMAL(3),
	"saves" int,
	"save_percentage" DECIMAL(3),
	"faceoffs_won" int
) WITH (
  OIDS=FALSE
);



ALTER TABLE "team" ADD CONSTRAINT "team_fk0" FOREIGN KEY ("arena_id") REFERENCES "arena"("arena_id");



ALTER TABLE "player_team" ADD CONSTRAINT "player_team_fk0" FOREIGN KEY ("team_id") REFERENCES "team"("team_id");
ALTER TABLE "player_team" ADD CONSTRAINT "player_team_fk1" FOREIGN KEY ("player_id") REFERENCES "player"("player_id");

ALTER TABLE "match" ADD CONSTRAINT "match_fk0" FOREIGN KEY ("championship_id") REFERENCES "championship"("champion_id");
ALTER TABLE "match" ADD CONSTRAINT "match_fk1" FOREIGN KEY ("home_team_id") REFERENCES "team"("team_id");
ALTER TABLE "match" ADD CONSTRAINT "match_fk2" FOREIGN KEY ("away_team_id") REFERENCES "team"("team_id");
ALTER TABLE "match" ADD CONSTRAINT "match_fk3" FOREIGN KEY ("location_id") REFERENCES "arena"("arena_id");

ALTER TABLE "match_events" ADD CONSTRAINT "match_events_fk0" FOREIGN KEY ("match_id") REFERENCES "match"("match_id");

ALTER TABLE "goals" ADD CONSTRAINT "goals_fk0" FOREIGN KEY ("match_id") REFERENCES "match"("match_id");
ALTER TABLE "goals" ADD CONSTRAINT "goals_fk1" FOREIGN KEY ("goalscorer_id") REFERENCES "player"("player_id");

ALTER TABLE "goal_assists" ADD CONSTRAINT "goal_assists_fk0" FOREIGN KEY ("assist_player_id") REFERENCES "player"("player_id");
ALTER TABLE "goal_assists" ADD CONSTRAINT "goal_assists_fk1" FOREIGN KEY ("goal_id") REFERENCES "goals"("goal_id");


ALTER TABLE "match_player_stats" ADD CONSTRAINT "match_player_stats_fk0" FOREIGN KEY ("match_id") REFERENCES "match"("match_id");
ALTER TABLE "match_player_stats" ADD CONSTRAINT "match_player_stats_fk1" FOREIGN KEY ("team_id") REFERENCES "team"("team_id");
ALTER TABLE "match_player_stats" ADD CONSTRAINT "match_player_stats_fk2" FOREIGN KEY ("player_id") REFERENCES "player"("player_id");

ALTER TABLE "match_team_stats" ADD CONSTRAINT "match_team_stats_fk0" FOREIGN KEY ("match_id") REFERENCES "match"("match_id");
ALTER TABLE "match_team_stats" ADD CONSTRAINT "match_team_stats_fk1" FOREIGN KEY ("team_id") REFERENCES "team"("team_id");












