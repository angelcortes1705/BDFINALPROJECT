-- Keyspace
CREATE KEYSPACE IF NOT EXISTS fraudks WITH replication = {'class':'SimpleStrategy','replication_factor':1};

USE fraudks;

-- transactions_by_account
CREATE TABLE IF NOT EXISTS transactions_by_account (
  account_id uuid,
  bucket int,               -- e.g. YYYYMM
  txn_time timestamp,
  txn_id uuid,
  amount decimal,
  currency text,
  merchant text,
  device_id uuid,
  ip_id uuid,
  raw_json text,
  flags set<text>,
  PRIMARY KEY ((account_id, bucket), txn_time, txn_id)
) WITH CLUSTERING ORDER BY (txn_time DESC, txn_id ASC);

-- transactions_by_device
CREATE TABLE IF NOT EXISTS transactions_by_device (
  device_id uuid,
  bucket int,
  txn_time timestamp,
  txn_id uuid,
  account_id uuid,
  amount decimal,
  merchant text,
  raw_json text,
  PRIMARY KEY ((device_id, bucket), txn_time, txn_id)
) WITH CLUSTERING ORDER BY (txn_time DESC, txn_id ASC);

-- transactions_by_ip
CREATE TABLE IF NOT EXISTS transactions_by_ip (
  ip_id uuid,
  bucket int,
  txn_time timestamp,
  txn_id uuid,
  account_id uuid,
  amount decimal,
  merchant text,
  raw_json text,
  PRIMARY KEY ((ip_id, bucket), txn_time, txn_id)
) WITH CLUSTERING ORDER BY (txn_time DESC, txn_id ASC);

-- alerts_timeline
CREATE TABLE IF NOT EXISTS alerts_timeline (
  alert_day date,
  severity text,
  alert_time timestamp,
  alert_id uuid,
  summary text,
  related_txns set<uuid>,
  status text,
  PRIMARY KEY ((alert_day), severity, alert_time, alert_id)
) WITH CLUSTERING ORDER BY (severity ASC, alert_time DESC);

-- alerts_by_rule
CREATE TABLE IF NOT EXISTS alerts_by_rule (
  rule_id uuid,
  bucket int,
  alert_time timestamp,
  alert_id uuid,
  severity text,
  summary text,
  related_txns set<uuid>,
  status text,
  PRIMARY KEY ((rule_id, bucket), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id ASC);

-- upload_monitor
CREATE TABLE IF NOT EXISTS upload_monitor (
  upload_day date,
  upload_id uuid,
  start_time timestamp,
  end_time timestamp,
  status text,
  user_id uuid,
  PRIMARY KEY ((upload_day), upload_id)
);
