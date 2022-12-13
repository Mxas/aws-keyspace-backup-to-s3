# aws-keyspace-backup-to-s3
(JAR can be found in build/libs/keystore-backup.jar)

Command line Tool for Keyspace CSV backups

1. Reading whole table (or provided query) and posting in -in-memory queue.
2. Storing lines in CSV file and
    1. Uploading in S3 bucket (with multi-part upload)
    2. Storing locally in provided file

AWS keystore config file example:

```
datastax-java-driver {
basic {
  load-balancing-policy {
    local-datacenter = us-east-2
  }
  contact-points = ["cassandra.us-east-2.amazonaws.com:9142"]
  request {
  page-size = 20000
  timeout = 140 seconds
  consistency = LOCAL_QUORUM
 }
} 
advanced {
 control-connection {
  timeout = 40 seconds
}
 connection {
 connect-timeout = 40 seconds
 init-query-timeout = 40 seconds
}
 auth-provider {
  class = PlainTextAuthProvider
  username = "my-user
  password = "my-pass"
 }
 ssl-engine-factory {
 class = DefaultSslEngineFactory
 truststore-password = "my-truststore-jks-pass"
 truststore-path = "truststore.jks"
 }
 metadata {
 token-map.enabled = false
 schema.enabled = true 
 }
 }
}
```

Pre req.
Java 11

Tool is not fully finished, still development in progress.

Tool arguments (optional)

      -command,--command                                   Skip menu and execute [backup, restore, reinsert, delete] command. Otherwise use interactive menu.
      -fs,--fs-result-path                                 Path where to store files locally.
      -imq,--in-memory-queue                               In memory blocking queue rows size.
      -ims,--in-memory-stream-size-mb                      In memory buffered stream size in MB. If AWS s3 storage will be used, then this size will be one multi part size value in MB.
      -imtime,--in-memory-queue-poll-timeout-secs          In memory buffered stream size building from queue polling timeout in seconds.
      -kdelb,--keyspace-delete-batch-size                  AWS Keyspace delete batch size (aws max30).
      -ke,--keyspace-empty-to-finish                       AWS Keyspace returned empty pages assume as finished (max int).
      -kerrp,--keyspace-stop-after-error-pages-count       AWS Keyspace stop execution after errored/failed pages fetching.
      -kf,--keyspace-config-file                           AWS Keyspace configuration file path.
      -kk,--keyspace-keyspace                              AWS Keyspace storage 'keyspace'. If query will be provided this value will be ignored.
      -kt,--keyspace-table                                 AWS Keyspace storage 'table'. If query will be provided this value will be ignored.
      -kp,--keyspace-pages-to-skip                         AWS Keyspace pages to skip.
      -kq,--keyspace-query                                 AWS Keyspace data fetching query. Will ignoring keyspace.table if this value provided.
      -kquewt,--keyspace-wait-item-in-queue-mins           AWS Keyspace operations wait records in queue time in minutes (15 min).
      -krate,--keyspace-update-rate-limiter-per-sec        AWS Keyspace modify rate limiter (500!).
      -kthrds,--keyspace-write-thread-counts               AWS Keyspace write (restore/reinsert/delete) threads count (default 8).
      -kttl,--keyspace-reinsert-ttl-value                  AWS Keyspace reinsert ttl value (15552000 = 1y).
      -s3b,--s3-bucket                                     AWS S3 bucket.
      -s3f,--s3-folder                                     AWS S3 folder (object prefix in bucket).
      -s3r,--s3-region                                     AWS S3 bucket region.
      -s3res,--s3-restore-from-csv-key                     AWS S3 file key to to restore from bucket (full path in bucket).
      -s3suf,--s3-store-file-suffix                        AWS S3 file to store suffix (<timestamp>_<suffix>.csv).
      -statheadr,--stat-reprint-header-after-seconds       Statistic header reprinting after seconds.
      -statline,--stat-print-in-new-line-after-secs        Statistic new line printing after seconds.
      -statstop,--stat-print-stop-after-no-changes-secs    Statistic printing stopping after not changes found.
      -stattime,--stat-update-timeout-in-mills             Statistic line refresh timeout in milliseconds.

Example startup command with default arguments

      java -jar keystore-backup.jar \
      -kq "select field,userid,deviceid,date,hour,minute,timestamp from my_keyspace.my_table where timestamp >= '2021-01-01T00:00:00.000Z' and timestamp < '2022-01-01T00:00:00.000Z' ALLOW FILTERING" \
      -kf "keyspace_prd.conf" \
      -kk my_keyspace \
      -kt my_table \
      -fs "v1_dump.csv" \
      -s3b my-buscket \
      -s3f my-data-2021-data \
      -s3r us-east-1

Backup command

      java -jar keystore-backup.jar \
      -command backup \
      --keyspace-config-file "test.conf" \
      --keyspace-keyspace my_keyspace \
      --keyspace-table my_table \
      --s3-bucket mk-app-test \
      --s3-folder small-test \
      --s3-store-file-suffix all-data \
      --s3-region us-east-1 \
      --stat-print-stop-after-no-changes-secs 45 \
      --in-memory-queue-poll-timeout-secs 5



TODO:
1. Exception handling clean up
2. Exception recovery on data fetching
3. Logs clean up