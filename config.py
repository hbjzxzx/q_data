from prefect.filesystems import RemoteFileSystem

minio_block = RemoteFileSystem(
    basepath="s3://raw/stock_meta_infos",
    settings={
        "key": "maxEVbfHYRqQKcMJnLct",
        "secret": "CTzIrX0YZbXC6ULiUFLMidCIq7vFKRy07N6cSDn9",
        "client_kwargs": {"endpoint_url": "http://localhost:9000"},
    },
)
minio_block.save("bucket")

