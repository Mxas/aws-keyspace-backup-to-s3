# aws-keyspace-backup-to-s3
Command line Tool for Keyspace CSV backups


1. Reading whole table (or provided query) and posting in -in-memory queue.
2. Storing lines in VCS file and
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

      -fs,--fs-result-path               Path where to store files locally.
      -imq,--in-memory-queue             In memory blocking queue rows size.
      -ims,--in-memory-stream-size-mb    In memory buffered stream size in MB.
      -kf,--keyspace-config-file         AWS Keyspace configuration file path.
      -kk,--keyspace-keyspace            AWS Keyspace storage 'keyspace'. If query will be provided this value will be ignored.
      -kq,--keyspace-query               AWS Keyspace data fetching query. Will ignoring keyspace.table if this value provided.
      -kt,--keyspace-table               AWS Keyspace storage 'table'. If query will be provided this value will be ignored.
      -m,--menu                          Use interactive menu.
      -s3b,--s3-bucket                   AWS S3 bucket.
      -s3f,--s3-folder                   AWS S3 folder (object prefix in bucket).
      -s3r,--s3-region                   AWS S3 bucket region.
