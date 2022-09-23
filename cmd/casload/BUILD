go_package(name="src")

go_binary(
    name="bin",
    output_path="casload",
)

docker_image(
    name="casload",
    dependencies=[":bin"],
    repository="toolchainlabs/remote-api-tools-docker",
    instructions=[
        "FROM alpine:3.13.2",
        "COPY casload /usr/local/bin/",
        'ENTRYPOINT ["/usr/local/bin/casload"]',
    ],
)
