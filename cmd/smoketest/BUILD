go_package()

go_binary(
    name="bin",
    output_path="smoketest",
)
docker_image(
    name="smoketest-docker",
    dependencies=[":bin"],
    repository="toolchainlabs/remote-api-tools-smoketest",
    instructions=[
        "FROM alpine:3.13.2",
        "COPY smoketest /usr/local/bin/",
        'ENTRYPOINT ["/usr/local/bin/smoketest"]',
    ],
)
