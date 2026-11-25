-- journal's tables. The migration runner pins search_path to the
-- configured schema, so no statement names a schema.

CREATE SEQUENCE commit_seq AS bigint MINVALUE 1 NO CYCLE;

CREATE TABLE events (
    event_id      bigint PRIMARY KEY CHECK (event_id > 0),
    team_id       bigint NOT NULL CHECK (team_id > 0),
    kind          text NOT NULL,
    publisher     text NOT NULL,
    publisher_key text NOT NULL,
    publisher_seq bigint NOT NULL CHECK (publisher_seq > 0),
    task_id       bigint,
    attempt       bigint,
    claim_seq     bigint,
    executor_id   bigint,
    control_id    bigint,
    occurred_at   timestamptz NOT NULL,
    committed_at  timestamptz NOT NULL,
    stream_seq    bigint NOT NULL DEFAULT 0,
    data          jsonb NOT NULL,
    UNIQUE (publisher_key, publisher_seq)
);
CREATE INDEX events_team_task ON events (team_id, task_id, event_id);
CREATE INDEX events_team_executor ON events (team_id, executor_id, event_id);

CREATE FUNCTION events_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'journal stored events are immutable';
END
$$;
CREATE TRIGGER events_no_change BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_immutable();
CREATE TRIGGER events_no_truncate BEFORE TRUNCATE ON events
    FOR EACH STATEMENT EXECUTE FUNCTION events_immutable();

CREATE TABLE rejected (
    rejected_id   bigserial PRIMARY KEY,
    publisher_key text NOT NULL,
    publisher_seq bigint NOT NULL,
    subject       text NOT NULL,
    reason        text NOT NULL,
    rejected_at   timestamptz NOT NULL
);
CREATE UNIQUE INDEX rejected_message ON rejected (publisher_key, publisher_seq)
    WHERE publisher_key <> '' AND publisher_seq > 0;

CREATE TABLE frames (
    frame_id    bigint PRIMARY KEY CHECK (frame_id > 0),
    team_id     bigint NOT NULL,
    frame_type  text NOT NULL,
    task_id     bigint,
    executor_id bigint,
    control_id  bigint,
    data        jsonb NOT NULL
);
CREATE INDEX frames_team ON frames (team_id, frame_id);

CREATE TABLE task_links (
    team_id   bigint NOT NULL,
    task_id   bigint NOT NULL,
    event_id  bigint NOT NULL REFERENCES events (event_id),
    attempt   bigint NOT NULL,
    claim_seq bigint NOT NULL,
    PRIMARY KEY (team_id, task_id, event_id)
);

CREATE TABLE claims (
    team_id     bigint NOT NULL,
    task_id     bigint NOT NULL,
    attempt     bigint NOT NULL,
    claim_seq   bigint NOT NULL,
    executor_id bigint NOT NULL,
    event_id    bigint NOT NULL REFERENCES events (event_id),
    ended       boolean NOT NULL DEFAULT false,
    PRIMARY KEY (team_id, task_id, attempt, claim_seq)
);
CREATE INDEX claims_executor_open ON claims (executor_id) WHERE NOT ended;

CREATE TABLE held (
    event_id    bigint PRIMARY KEY REFERENCES events (event_id),
    team_id     bigint NOT NULL,
    task_id     bigint NOT NULL,
    attempt     bigint NOT NULL,
    claim_seq   bigint NOT NULL,
    executor_id bigint NOT NULL,
    kind        text NOT NULL,
    stored_at   timestamptz NOT NULL,
    status      text NOT NULL CHECK (status IN ('held', 'accepted', 'not_claim_holder')),
    log_offset  bigint,
    frame_id    bigint,
    payload     jsonb
);
CREATE INDEX held_waiting ON held (team_id, task_id, attempt, claim_seq) WHERE status = 'held';
CREATE INDEX held_age ON held (stored_at) WHERE status = 'held';

CREATE TABLE log_offsets (
    team_id     bigint NOT NULL,
    task_id     bigint NOT NULL,
    last_offset bigint NOT NULL,
    PRIMARY KEY (team_id, task_id)
);

CREATE TABLE logs (
    event_id      bigint PRIMARY KEY,
    team_id       bigint NOT NULL,
    task_id       bigint NOT NULL,
    attempt       bigint NOT NULL,
    claim_seq     bigint NOT NULL,
    executor_id   bigint NOT NULL,
    log_offset    bigint NOT NULL,
    stream        text NOT NULL,
    text          text NOT NULL,
    publisher_seq bigint NOT NULL,
    occurred_at   timestamptz NOT NULL,
    UNIQUE (team_id, task_id, log_offset)
);

CREATE TABLE executors (
    executor_id           bigint PRIMARY KEY,
    team_id               bigint NOT NULL,
    tag                   text NOT NULL,
    executor_type         text NOT NULL,
    registered            boolean NOT NULL,
    removed               boolean NOT NULL,
    connection_state      text NOT NULL,
    connection_changed_at timestamptz,
    held_claims           bigint,
    available_capacity    bigint,
    max_parallel_tasks    bigint,
    reported_ip           text,
    ip_changed_at         timestamptz
);
CREATE INDEX executors_team ON executors (team_id, executor_id);

CREATE TABLE tasks (
    team_id          bigint NOT NULL,
    task_id          bigint NOT NULL,
    first_queued_at  timestamptz,
    first_queued_id  bigint,
    latest_kind      text NOT NULL,
    latest_attempt   bigint NOT NULL,
    latest_claim_seq bigint NOT NULL,
    latest_event_id  bigint NOT NULL,
    PRIMARY KEY (team_id, task_id)
);
CREATE INDEX tasks_first_queued ON tasks (team_id, first_queued_at);
