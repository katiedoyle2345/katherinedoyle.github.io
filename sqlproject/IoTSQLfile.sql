/*DROP TABLE IF EXISTS network_traffic;
CREATE TABLE network_traffic (
	id_row VARCHAR(50),
    id_orig_h VARCHAR(50),
    id_orig_p INTEGER,
    id_resp_h VARCHAR(50),
    id_resp_p INTEGER,
    proto VARCHAR(10),
    service VARCHAR(50),
    duration DECIMAL,
    orig_bytes BIGINT,
    resp_bytes BIGINT,
    conn_state VARCHAR(20),
    missed_bytes BIGINT,
    history TEXT,
    orig_pkts BIGINT,
    orig_ip_bytes BIGINT,
    resp_pkts BIGINT,
    resp_ip_bytes BIGINT,
    label VARCHAR(50)
);

copy network_traffic FROM '/Users/katie/IoTNet24 Dataset for IDS.csv' DELIMITER ',' CSV HEADER;

--Data Cleaning--

--Handleling Missing Values--
UPDATE network_traffic SET orig_pkts = COALESCE(orig_pkts, 0);
UPDATE network_traffic SET orig_ip_bytes = COALESCE(orig_ip_bytes, 0);
UPDATE network_traffic SET resp_ip_bytes = COALESCE(resp_ip_bytes, 0);

SELECT COUNT(*) FROM network_traffic WHERE id_resp_p IS NULL OR proto IS NULL OR conn_state IS NULL;

--Remove Duplicates--

DELETE FROM network_traffic
WHERE ctid NOT IN (
  SELECT MIN(ctid)
  FROM network_traffic
  GROUP BY network_traffic.*
);
--Anomaly Detection:Irregular Connection States

SELECT conn_state, COUNT(*) AS count
FROM network_traffic
WHERE conn_state NOT IN ('S0', 'SF', 'REJ')  -- assuming 'S0', 'SF', 'REJ' are normal states
GROUP BY conn_state;

--Protocol and Port Utilisation :Frequent Ports Analysis
SELECT id_resp_p, proto, COUNT(*) AS connections
FROM network_traffic
GROUP BY id_resp_p, proto
ORDER BY connections DESC
LIMIT 10; -- Top 10 most used ports

--Port Behavior: Port Traffic Patterns:'id_resp_p' identifies each port

SELECT id_resp_p, AVG(orig_ip_bytes) AS avg_bytes_sent, COUNT(*) AS total_sessions
FROM network_traffic
GROUP BY id_resp_p
ORDER BY avg_bytes_sent DESC;

--Detailed Anomaly Report by Device and Protocol
WITH Anomalies AS (
  SELECT  id_resp_p, proto, SUM(orig_ip_bytes) AS total_bytes, COUNT(*) AS sessions
  FROM network_traffic
  GROUP BY  id_resp_p, proto
)
SELECT A.id_resp_p, A.proto, A.total_bytes, A.sessions
FROM Anomalies A
WHERE A.sessions > (SELECT AVG(sessions) * 1.5 FROM Anomalies)
ORDER BY A.total_bytes DESC;

--Overall Performance and Effectiveness: KPI Metrics for Network Traffic

SELECT proto,
       AVG(orig_pkts) AS average_packets,
       MIN(orig_pkts) AS min_packets,
       MAX(orig_pkts) AS max_packets,
       AVG(resp_ip_bytes) AS average_response_bytes
FROM network_traffic
GROUP BY proto;

--Feature Enginnering: Encode Categorical Features: Convert categorical variables like proto and conn_state into numerical values using techniques like one-hot encoding. -

ALTER TABLE network_traffic ADD COLUMN proto_tcp INTEGER;
ALTER TABLE network_traffic ADD COLUMN proto_udp INTEGER;

UPDATE network_traffic
SET proto_tcp = CASE WHEN proto = 'TCP' THEN 1 ELSE 0 END,
    proto_udp = CASE WHEN proto = 'UDP' THEN 1 ELSE 0 END;

--Create New features:create a feature representing the ratio of orig_ip_bytes to orig_pkts to see how many bytes are sent per packet on average.--

ALTER TABLE network_traffic ADD COLUMN avg_bytes_per_packet DECIMAL;

UPDATE network_traffic
SET avg_bytes_per_packet = CASE 
    WHEN orig_pkts > 0 THEN orig_ip_bytes / orig_pkts 
    ELSE NULL 
END;

--Feature Scaling:
ALTER TABLE network_traffic ADD COLUMN scaled_orig_ip_bytes DECIMAL;
WITH minmax AS (SELECT MIN(orig_ip_bytes) AS min_bytes, MAX(orig_ip_bytes) AS max_bytes FROM network_traffic)
UPDATE network_traffic
SET scaled_orig_ip_bytes = (orig_ip_bytes - (SELECT min_bytes FROM minmax)) / ((SELECT max_bytes FROM minmax) - (SELECT min_bytes FROM minmax));
--Encode Categorical Variables:

ALTER TABLE network_traffic ADD COLUMN proto_code INTEGER;
UPDATE network_traffic SET proto_code = CASE
  WHEN proto = 'tcp' THEN 1
  WHEN proto = 'udp' THEN 2
  ELSE NULL
END;

--Data Partitioning:Partition the data into training and testing sets

CREATE TEMP TABLE training_data AS
SELECT * FROM network_traffic
WHERE RANDOM() <= 0.8;

CREATE TEMP TABLE testing_data AS
SELECT * FROM network_traffic
WHERE NOT EXISTS (SELECT 1 FROM training_data WHERE training_data.ctid = network_traffic.ctid);

--Total Traffic by Protocol--

SELECT proto, COUNT(*) AS total_connections
FROM network_traffic
GROUP BY proto;

--Average Packets Sent by Protocol--

SELECT proto, AVG(orig_pkts) AS avg_packets_sent
FROM network_traffic
GROUP BY proto;

--Statistical Analysis- Descriptive Statistics: 
SELECT
  AVG(orig_ip_bytes) as avg_orig_ip_bytes,
  STDDEV(orig_ip_bytes) as stddev_orig_ip_bytes,
  AVG(resp_ip_bytes) as avg_resp_ip_bytes,
  STDDEV(resp_ip_bytes) as stddev_resp_ip_bytes
FROM network_traffic;

--Correlation Analysis 
SELECT corr(orig_pkts, orig_ip_bytes) as correlation_orig,
       corr(resp_pkts, resp_ip_bytes) as correlation_resp
FROM network_traffic;

--Heat Map Analysis preparation for python
 SELECT proto, id_resp_p, COUNT(*) AS traffic_count
 FROM network_traffic
 GROUP BY proto, id_resp_p
 ORDER BY traffic_count DESC
*/