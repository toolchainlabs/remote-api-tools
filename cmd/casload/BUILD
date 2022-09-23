go_package()

go_binary(
    name="bin",
    output_path="casload",
)

docker_image(
    name="casload-docker",
    dependencies=[":bin"],
    repository="toolchainlabs/remote-api-tools-casload",
    instructions=[
        "FROM alpine:3.13.2",
        "COPY casload /usr/local/bin/",
        'ENTRYPOINT ["/usr/local/bin/casload"]',
    ],
)
